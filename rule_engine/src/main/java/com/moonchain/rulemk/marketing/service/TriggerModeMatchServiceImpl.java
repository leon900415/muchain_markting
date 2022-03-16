package com.moonchain.rulemk.marketing.service;

import com.moonchain.rulemk.marketing.beans.EventBean;
import com.moonchain.rulemk.marketing.beans.EventCombinationCondition;
import com.moonchain.rulemk.marketing.dao.ClickHouseQuerier;
import com.moonchain.rulemk.marketing.dao.HbaseQuerier;
import com.moonchain.rulemk.marketing.dao.StateQuerier;
import com.moonchain.rulemk.marketing.utils.ConfigNames;
import com.moonchain.rulemk.marketing.utils.ConnectionUtils;
import com.moonchain.rulemk.marketing.utils.CrossTimeQueryUtil;
import com.moonchain.rulemk.marketing.utils.EventUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.client.Connection;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.util.Map;

/**
 * 触发类型匹配接口
 *
 * @author: Moon-Chain 2022-03-07 19:38
 */
public class TriggerModeMatchServiceImpl {

  ClickHouseQuerier clickHouseQuerier;
  HbaseQuerier hbaseQuerier;
  StateQuerier stateQuerier;

  /**
   * 构造方法
   *
   * @param eventBeanListState flink的state缓存
   * @throws Exception 异常
   */
  public TriggerModeMatchServiceImpl(ListState<EventBean> eventBeanListState) throws Exception {
    Config config = ConfigFactory.load();
    // ck的查询对象
    ClickHouseConnection ckConn = ConnectionUtils.getClickhouserConnection();
    clickHouseQuerier = new ClickHouseQuerier(ckConn);
    // hbase的查询对象
    Connection hbaseConnection = ConnectionUtils.getHbaseConnection();
    hbaseQuerier =
        new HbaseQuerier(
            hbaseConnection,
            config.getString(ConfigNames.HBASE_TABLE_NAME),
            config.getString(ConfigNames.HBASE_COLUMN_FAMILY_NAME));
    // state的查询对象
    stateQuerier = new StateQuerier(eventBeanListState);
  }

  /**
   * 用户画像规则匹配
   *
   * @param deviceId 事件id
   * @param userProfileConditions 用户画像规则
   * @return 是否匹配
   * @throws Exception 异常
   */
  public boolean matchProfileCondition(Map<String, String> userProfileConditions, String deviceId)
      throws Exception {
    return hbaseQuerier.queryProfileConditionIsMatch(userProfileConditions, deviceId);
  }

  /**
   * 计算[一个]行为组是否符合条件
   *
   * @param eventCombinationCondition 行为组规则
   * @param eventBean 事件bean
   * @return 是否符合条件
   */
  public boolean matchEventCombinationCondition(
      EventCombinationCondition eventCombinationCondition, EventBean eventBean) throws Exception {
    // 当前时间对应的时间分割点
    long segmentPoint = CrossTimeQueryUtil.getSegmentPoint(eventBean.getTimeStamp());
    // 条件开始时间
    long timeRangeStart = eventCombinationCondition.getTimeRangeStart();
    // 条件结束时间
    long timeRangeEnd = eventCombinationCondition.getTimeRangeEnd();
    if (timeRangeStart > segmentPoint) {
      // 查询状态
      int stateCount =
          stateQuerier.queryEventCombinationConditionCount(
              eventBean.getDeviceId(), eventCombinationCondition, timeRangeStart, timeRangeEnd);
      return (stateCount >= eventCombinationCondition.getMinLimit()
          && stateCount <= eventCombinationCondition.getMaxLimit());

    } else if (timeRangeEnd < segmentPoint) {
      // 查询ck
      Tuple2<String, Integer> sequenceCount =
          clickHouseQuerier.queryEventCombinationConditionCount(
              eventBean.getDeviceId(),
              eventCombinationCondition,
              timeRangeStart,
              timeRangeEnd,
              true);
      return (sequenceCount.f1 >= eventCombinationCondition.getMinLimit()
          && sequenceCount.f1 <= eventCombinationCondition.getMaxLimit());
    } else {
      // 跨界
      // 先查询一次state 看能否提前结束
      int stateCount =
          stateQuerier.queryEventCombinationConditionCount(
              eventBean.getDeviceId(), eventCombinationCondition, segmentPoint, timeRangeEnd);
      if (stateCount >= eventCombinationCondition.getMinLimit()) {
        return true;
      }
      // 再拼正则str
      String step1Str =
          stateQuerier.getEventCombineConditionStr(
              eventBean.getDeviceId(), eventCombinationCondition, segmentPoint, timeRangeEnd);

      Tuple2<String, Integer> sequenceCount =
          clickHouseQuerier.queryEventCombinationConditionCount(
              eventBean.getDeviceId(),
              eventCombinationCondition,
              timeRangeStart,
              segmentPoint,
              true);
      int conditionCout =
          EventUtil.eventStrMatchGroupCount(
              step1Str + sequenceCount.f0, eventCombinationCondition.getMatchPattern());
      return (conditionCout >= eventCombinationCondition.getMinLimit()
          && conditionCout <= eventCombinationCondition.getMaxLimit());
    }
  }
}
