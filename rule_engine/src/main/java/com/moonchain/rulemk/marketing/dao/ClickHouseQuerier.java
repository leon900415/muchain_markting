package com.moonchain.rulemk.marketing.dao;

import com.moonchain.rulemk.marketing.beans.BufferData;
import com.moonchain.rulemk.marketing.beans.EventCombinationCondition;
import com.moonchain.rulemk.marketing.beans.EventCondition;
import com.moonchain.rulemk.marketing.buffer.BufferManagerImpl;
import com.moonchain.rulemk.marketing.utils.ConfigNames;
import com.moonchain.rulemk.marketing.utils.EventUtil;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * clickhouse的查询服务类
 *
 * @author: Moon-Chain 2022-03-07 19:40
 */
public class ClickHouseQuerier {
  private final ClickHouseConnection ckConn;
  private final BufferManagerImpl bufferManager;
  long bufferTtl;

  public ClickHouseQuerier(ClickHouseConnection ckConn) {
    this.ckConn = ckConn;
    bufferManager = new BufferManagerImpl();
    bufferTtl = ConfigFactory.load().getLong(ConfigNames.REDIS_BUFFER_TTL);
  }

  /**
   * 在cLickhouse中，根据组合条伴及查询的时问范園，得到返回结果的1213形式字符串序列
   *
   * @param eventCombinationCondition 行为组合条件
   * @param queryRangeStart 查询时间范園开始
   * @return 用户做过的组合条件中的字符串序列
   * @panam queryRangeEnd 查询时间范围结來
   */
  public String getEventCombineConditionStr(
      String deviceId,
      EventCombinationCondition eventCombinationCondition,
      long queryRangeStart,
      long queryRangeEnd)
      throws Exception {
    String querySql = eventCombinationCondition.getQuerySql();
    PreparedStatement preparedStatement = ckConn.prepareStatement(querySql);
    preparedStatement.setString(1, deviceId);
    preparedStatement.setLong(2, queryRangeStart);
    preparedStatement.setLong(3, queryRangeEnd);
    ResultSet resultSet = preparedStatement.executeQuery();
    List<EventCondition> eventConditionList = eventCombinationCondition.getEventConditionList();
    List<String> eventIdList =
        eventConditionList.stream().map(EventCondition::getEventId).collect(Collectors.toList());
    StringBuilder sb = new StringBuilder();
    while (resultSet.next()) {
      String eventId = resultSet.getString(1);
      sb.append(eventIdList.indexOf(eventId) + 1);
    }
    return sb.toString();
  }

  /**
   * @param deviceId 事件id
   * @param eventCombinationCondition 行为组合条件
   * @param queryRangeStart 查询时间范園开始
   * @param queryRangeEnd 查询时间范围结来
   * @return 出现的次数
   * @throws Exception 异常
   */
  public Tuple2<String, Integer> queryEventCombinationConditionCount(
      String deviceId,
      EventCombinationCondition eventCombinationCondition,
      long queryRangeStart,
      long queryRangeEnd)
      throws Exception {
    return queryEventCombinationConditionCount(
        deviceId, eventCombinationCondition, queryRangeStart, queryRangeEnd, false);
  }

  /**
   * @param deviceId 事件id
   * @param eventCombinationCondition 行为组合条件
   * @param queryRangeStart 查询时间范園开始
   * @param queryRangeEnd 查询时间范围结来
   * @param needWholeStr 是否需要完整的缓存序列字符串
   * @return 出现的次数
   */
  public Tuple2<String, Integer> queryEventCombinationConditionCount(
      String deviceId,
      EventCombinationCondition eventCombinationCondition,
      long queryRangeStart,
      long queryRangeEnd,
      boolean needWholeStr)
      throws Exception {
    /* 获取缓存数据
     * 缓存数据的时间范围： [t3 -> t8]
     * 查询条件的时间范围：
     * [t3 -> t8] 直接用缓存的结果直接作为方法的返回值
     * [t3 -＞t10〕判断缓存数据的count值是否＞=条件的count阈值，如果成立，则直接返回缓存结果；否则，用缓存的结果 + t8->t10的查询结果 作为整个返回结果
     * [t1 -> t8] 判断缓存数据的count值是否＞=条件的count阈值，如果成立，则直接返回缓存结果；否则，用缓存的结果 + t1->t3的查询结果 作为整个返回结果
     * [t1 -> t10〕 判断缓存数据的count值是否 ＞= 条件的count阚值，如果成立，则直接返回缓存结果；否则，无用
     * 如下逻辑还需考虑一个问题
     * valueMap同时存在多种缓存区间  应该择优选取更合适的缓存区间
     */
    String bufferKey = deviceId + ":" + eventCombinationCondition.getCacheId();
    BufferData dataFromBuffer = bufferManager.getDataFromBuffer(bufferKey);
    Map<String, String> valueMap = dataFromBuffer.getValueMap();
    /**
     * key deviceld:cacheld
     *
     * <p>valueMap key value 2:6:20 AAABDCC 2:8:10 ADDFSDG 0:6:30 AAACCF
     */
    /*当前时间*/
    long current = System.currentTimeMillis();
    for (String key : valueMap.keySet()) {
      String[] hsetKey = key.split(":");

      long bufferInstertTime = Long.parseLong(hsetKey[2]);
      long bufferStartTime = Long.parseLong(hsetKey[0]);
      long bufferEndTime = Long.parseLong(hsetKey[1]);
      /*
       * 判断缓存是否过期,做清除操作
       */
      if (bufferInstertTime > bufferTtl) {
        bufferManager.delBufferEntry(bufferKey, hsetKey);
      }

      String bufferSequenceStr = valueMap.get(key);
      int bufferCount =
          EventUtil.eventStrMatchGroupCount(
              bufferSequenceStr, eventCombinationCondition.getMatchPattern());
      // 查询范围跟缓存时间范围完全一致
      if (queryRangeStart == bufferStartTime && queryRangeEnd == bufferEndTime) {
        // 更新buffer缓存的insertTime
        bufferManager.delBufferEntry(bufferKey, hsetKey);
        Map<String, String> toPutMap = new HashMap<>();
        toPutMap.put(bufferStartTime + ":" + bufferEndTime + ":" + current, bufferSequenceStr);
        bufferManager.putDataToBuffer(bufferKey, toPutMap);
        return Tuple2.of(bufferSequenceStr, bufferCount);
      }
      /*
       * 左端点对齐
       * 查询范围的endTime > bufferEndTime
       */
      if (queryRangeStart == bufferStartTime && queryRangeEnd > bufferEndTime) {

        // 最小值
        int minLimit = eventCombinationCondition.getMinLimit();
        /*
        不需要完整的str needWholeStr
         */
        if (bufferCount >= minLimit && !needWholeStr) {
          return Tuple2.of(bufferSequenceStr, bufferCount);
        } else {
          /*删除缓存*/
          bufferManager.delBufferEntry(bufferKey, hsetKey);
          String querySequenceStr =
              getEventCombineConditionStr(
                  deviceId, eventCombinationCondition, bufferEndTime, queryRangeEnd);
          // 将查询的结果存入缓存
          Map<String, String> toPutMap = new HashMap<>();
          /*
           * 范围如下
           * |---缓存---|
           *            |--ck---|
           * |------全部---------|
           */
          toPutMap.put(bufferStartTime + ":" + bufferEndTime + ":" + current, bufferSequenceStr);
          toPutMap.put(bufferEndTime + ":" + queryRangeEnd + ":" + current, querySequenceStr);
          toPutMap.put(
              bufferStartTime + ":" + queryRangeEnd + ":" + current,
              bufferSequenceStr + querySequenceStr);
          bufferManager.putDataToBuffer(bufferKey, toPutMap);
          return Tuple2.of(bufferSequenceStr + querySequenceStr, bufferCount);
        }
      }

      /*
       * 右端点对齐
       * 查询范围的queryStartTime > bufferEndTime
       */
      if (queryRangeEnd == bufferEndTime && queryRangeStart < bufferStartTime) {

        if (bufferCount >= eventCombinationCondition.getMinLimit() && !needWholeStr) {
          return Tuple2.of(bufferSequenceStr, bufferCount);
        } else {
          // 删除缓存
          bufferManager.delBufferEntry(bufferKey, hsetKey);
          String querySequenceStr =
              getEventCombineConditionStr(
                  deviceId, eventCombinationCondition, queryRangeStart, bufferStartTime);
          /*
           * 范围如下
           *       |---缓存--|
           *|--ck--|
           *|------所有---------|
           */
          Map<String, String> toPutMap = new HashMap<>();
          toPutMap.put(bufferStartTime + ":" + bufferEndTime + ":" + current, bufferSequenceStr);
          toPutMap.put(queryRangeStart + ":" + bufferStartTime + ":" + current, querySequenceStr);
          toPutMap.put(
              queryRangeStart + ":" + bufferEndTime + ":" + current,
              querySequenceStr + bufferSequenceStr);
          bufferManager.putDataToBuffer(bufferKey, toPutMap);
          return Tuple2.of(querySequenceStr + bufferSequenceStr, bufferCount);
        }
      }

      /*
       * 查询范围大于bufferRange
       * 查询范围的queryStartTime < bufferEndTime  %% queryEndTime > bufferEndTime
       */
      if (queryRangeStart < bufferEndTime && queryRangeEnd > bufferStartTime) {
        // 将原buffer的从redis中删掉
        bufferManager.delBufferEntry(bufferKey, hsetKey);
        // 更新该缓存
        Map<String, String> toPutMap = new HashMap<>();
        toPutMap.put(bufferStartTime + ":" + bufferEndTime + ":" + current, bufferSequenceStr);
        bufferManager.putDataToBuffer(bufferKey, toPutMap);
        if (bufferCount >= eventCombinationCondition.getMinLimit() && !needWholeStr) {
          return Tuple2.of(bufferSequenceStr, bufferCount);
        }
      }
    }

    // 先查询到用户在组合条件中做过的事件的字符串序列
    String str =
        getEventCombineConditionStr(
            deviceId, eventCombinationCondition, queryRangeStart, queryRangeEnd);
    Map<String, String> toPutMap = new HashMap<>();
    toPutMap.put(queryRangeStart + ":" + queryRangeEnd + ":" + current, str);
    bufferManager.putDataToBuffer(bufferKey, toPutMap);
    int resultCount =
        EventUtil.eventStrMatchGroupCount(str, eventCombinationCondition.getMatchPattern());
    return Tuple2.of(str, resultCount);
  }
}
