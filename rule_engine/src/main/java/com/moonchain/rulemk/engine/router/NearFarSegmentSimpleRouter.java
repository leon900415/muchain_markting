package com.moonchain.rulemk.engine.router;

import com.moonchain.rulemk.datagen.UserProfileDataGen;
import com.moonchain.rulemk.engine.beans.EventBean;
import com.moonchain.rulemk.engine.beans.EventParam;
import com.moonchain.rulemk.engine.beans.EventSequenceParam;
import com.moonchain.rulemk.engine.beans.RuleCondition;
import com.moonchain.rulemk.engine.queryService.ClickhouseQueryServiceImpl;
import com.moonchain.rulemk.engine.queryService.HbaseQueryServiceImpl;
import com.moonchain.rulemk.engine.queryService.StateQueryServiceImpl;
import com.moonchain.rulemk.engine.utils.ConnectionUtils;
import com.moonchain.rulemk.engine.utils.EventParamComparator;
import com.moonchain.rulemk.engine.utils.SegmentQueryUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.hadoop.hbase.client.Connection;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.util.List;
import java.util.Map;

/**
 * ck时间分隔组合过滤
 *
 * @author: Moon-Chain 2022-03-05 18:37
 */
@Slf4j
public class NearFarSegmentSimpleRouter {

  private final HbaseQueryServiceImpl hbaseQueryService;
  private final ClickhouseQueryServiceImpl ckQueryService;
  private final StateQueryServiceImpl stateQueryService;

  public NearFarSegmentSimpleRouter(ListState<EventBean> eventBeanListState) throws Exception {
    /*
     * hbase的连接器
     * */
    Connection hbaseConnection = ConnectionUtils.getHbaseConnection();
    /*
     * hbase的查询实体
     * */
    hbaseQueryService = new HbaseQueryServiceImpl(hbaseConnection);
    /*
     * ck的连接器
     * */
    ClickHouseConnection clickhouserConnection = ConnectionUtils.getClickhouserConnection();
    /*
     * ck的查询实体
     * */
    ckQueryService = new ClickhouseQueryServiceImpl(clickhouserConnection);
    /*
     * flink的状态查询实体
     * */
    stateQueryService = new StateQueryServiceImpl(eventBeanListState);
  }

  /** 路由匹配过滤 */
  public boolean isMatch(
      RuleCondition rule, EventBean eventBean, ListState<EventBean> eventBeanStates)
      throws Exception {
    if (!EventParamComparator.compare(rule.getTiggerEvent(), eventBean)) {
      return false;
    }
    log.info(
        "规则被触发... 规则 {}, 事件 {},触发时间为 {}",
        rule.getRuleId(),
        rule.getTiggerEvent().getEventId(),
        System.currentTimeMillis());

    // 画像条件
    Map<String, String> userProfileConditions = rule.getUserProfileConditions();
    boolean userProfileMatch = false;
    if (userProfileConditions != null) {
      log.debug("用户画像条件不为空,开始查询画像条件...");
      userProfileMatch =
          hbaseQueryService.queryProfileCondition(
              eventBean.getDeviceId(),
              userProfileConditions,
              UserProfileDataGen.TABLE_NAME,
              UserProfileDataGen.COLUMNS_FAMILY_1);
      if (!userProfileMatch) {
        return false;
      }
    }

    /*
     * 获取时间分割点
     * */
    long segmentPoint = SegmentQueryUtil.getSegmentPoint(eventBean.getTimeStamp());
    // 行为次数
    List<EventParam> actionCountConditions = rule.getActionCountConditionsList();
    if (actionCountConditions != null && actionCountConditions.size() > 0) {
      log.debug("行为次数条件不为空,开始查询行为次数条件...");
      for (EventParam countCondition : actionCountConditions) {
        // 判断时间节点  如果在左边 做全部查询ck
        if (countCondition.getTimeRangeEnd() < segmentPoint) {
          // 如果查询到一个条件不满足,那么整个规则结束
          long count =
              ckQueryService.queryEventCountCondition(eventBean.getDeviceId(), countCondition);
          log.debug("条件阈值为 {}, 查询到的行为次数为 {}", countCondition.getCountThreshold(), count);
          if (count < countCondition.getCountThreshold()) {
            return false;
          }
        } else if (countCondition.getTimeRangeStart() > segmentPoint) {
          long actionCount =
              stateQueryEventCount(
                  eventBeanStates,
                  countCondition,
                  countCondition.getTimeRangeStart(),
                  countCondition.getTimeRangeEnd());
          if ((actionCount < countCondition.getCountThreshold())) {
            return false;
          }
        } else {
          /*
           * 跨界
           * 1. 先查状态
           * 2. 再查clickhouse
           * */
          long actionCount =
              stateQueryEventCount(
                  eventBeanStates, countCondition, segmentPoint, countCondition.getTimeRangeEnd());
          if ((actionCount < countCondition.getCountThreshold())) {
            // 再查ck
            long ckActionCount =
                ckQueryService.queryEventCountCondition(
                    eventBean.getDeviceId(),
                    countCondition,
                    countCondition.getTimeRangeStart(),
                    segmentPoint);
            if ((actionCount + ckActionCount < countCondition.getCountThreshold())) {
              return false;
            }
          }
        }
      }
    }

    // 行为序列
    log.debug("......准备查询行为序列条件...");
    List<EventSequenceParam> actionSequenceCondition = rule.getActionSequenceConditionList();
    if (actionSequenceCondition != null && actionSequenceCondition.size() > 0) {
      for (EventSequenceParam eventSequenceParam : actionSequenceCondition) {
        // 区分时间节点
        if (eventSequenceParam.getTimeRangeEnd() < segmentPoint) {
          // 全部查询ck
          // 如果查询到一个条件不满足,那么整个规则结束
          int maxStep =
              ckQueryService.queryEventSequence(eventBean.getDeviceId(), eventSequenceParam);
          if (maxStep < eventSequenceParam.getEventSequence().size()) {
            return false;
          }
        } else if (eventSequenceParam.getTimeRangeStart() > segmentPoint) {
          // 查询状态
          int step =
              stateQueryService.queryEventSequence(
                  eventSequenceParam.getEventSequence(),
                  eventSequenceParam.getTimeRangeStart(),
                  eventSequenceParam.getTimeRangeEnd());
          if (step < eventSequenceParam.getEventSequence().size()) {
            return false;
          }
        } else {
          /*
           * 跨界
           * */
          int step1 =
              ckQueryService.queryEventSequence(
                  eventBean.getDeviceId(),
                  eventSequenceParam,
                  eventSequenceParam.getTimeRangeStart(),
                  segmentPoint);
          int step2 = 0;
          if ((step1 < eventSequenceParam.getEventSequence().size())) {
            // 根据ck中的最大步数,来截断状态中的数据
            List<EventParam> eventSequence = eventSequenceParam.getEventSequence();
            List<EventParam> trimedEventSequence =
                eventSequence.subList(step1, eventSequence.size());
            // 查询state
            step2 =
                stateQueryService.queryEventSequence(
                    trimedEventSequence, segmentPoint, eventSequenceParam.getTimeRangeEnd());
          }
          if (step1 + step2 < eventSequenceParam.getEventSequence().size()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private long stateQueryEventCount(
      ListState<EventBean> eventBeanStates, EventParam countCondition, long timeStart, long timeEnd)
      throws Exception {
    long count = 0;
    // 判断时间节点  如果在左边 做全部查询listState
    for (EventBean bean : eventBeanStates.get()) {
      if (bean.getTimeStamp() >= timeStart && bean.getTimeStamp() <= timeEnd) {
        if (EventParamComparator.compare(countCondition, bean)) {
          count++;
        }
      }
    }
    return count;
  }
}
