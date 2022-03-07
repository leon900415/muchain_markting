package com.moonchain.rulemk.engine.router;

import com.moonchain.rulemk.datagen.UserProfileDataGen;
import com.moonchain.rulemk.engine.beans.EventBean;
import com.moonchain.rulemk.engine.beans.EventParam;
import com.moonchain.rulemk.engine.beans.EventSequenceParam;
import com.moonchain.rulemk.engine.beans.RuleCondition;
import com.moonchain.rulemk.engine.queryService.ClickhouseQueryServiceImpl;
import com.moonchain.rulemk.engine.queryService.HbaseQueryServiceImpl;
import com.moonchain.rulemk.engine.utils.ConnectionUtils;
import com.moonchain.rulemk.engine.utils.EventParamComparator;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.util.List;
import java.util.Map;

/**
 * 简单路由过滤
 *
 * @author: Moon-Chain 2022-03-04 16:28
 */
@Slf4j
public class SimpleQueryRouter {

  private final HbaseQueryServiceImpl hbaseQueryService;
  private final ClickhouseQueryServiceImpl ckQueryService;

  public SimpleQueryRouter() throws Exception {
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
  }

  /** 路由匹配过滤 */
  public boolean isMatch(RuleCondition rule, EventBean eventBean) throws Exception {
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


    // 行为次数
    List<EventParam> actionCountConditions = rule.getActionCountConditionsList();
    if (actionCountConditions != null && actionCountConditions.size() > 0) {
      log.debug("行为次数条件不为空,开始查询行为次数条件...");
      for (EventParam countCondition : actionCountConditions) {
        // 如果查询到一个条件不满足,那么整个规则结束
        long count =
            ckQueryService.queryEventCountCondition(eventBean.getDeviceId(), countCondition);
        log.debug("条件阈值为 {}, 查询到的行为次数为 {}", countCondition.getCountThreshold(), count);
        if (count < countCondition.getCountThreshold()) {
          return false;
        }
      }
    }

    // 行为序列
    log.debug("......准备查询行为序列条件...");
    List<EventSequenceParam> actionSequenceCondition = rule.getActionSequenceConditionList();
    if (actionSequenceCondition != null && actionSequenceCondition.size() > 0) {
      for (EventSequenceParam eventSequenceParam : actionSequenceCondition) {
        // 如果查询到一个条件不满足,那么整个规则结束
        int maxStep =
            ckQueryService.queryEventSequence(eventBean.getDeviceId(), eventSequenceParam);
        if (maxStep < eventSequenceParam.getEventSequence().size()) {
          return false;
        }
      }
    }
    return true;
  }
}
