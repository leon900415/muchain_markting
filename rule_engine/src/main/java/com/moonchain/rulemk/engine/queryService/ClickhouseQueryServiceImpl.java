package com.moonchain.rulemk.engine.queryService;

import com.moonchain.rulemk.engine.beans.EventParam;
import com.moonchain.rulemk.engine.beans.EventSequenceParam;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * ck的查询实现
 *
 * @author: Moon-Chain 2022-03-02 10:47
 */
@Slf4j
public class ClickhouseQueryServiceImpl implements QueryService {

  Connection ckConn;

  public ClickhouseQueryServiceImpl(Connection ckConn) {
    this.ckConn = ckConn;
  }

  /**
   * 查询行为次数 行为次数类规则条件的查询方法 如果不指定特定的查询时间范围，则用查询条件里面的时同范围
   *
   * @param deviceId
   * @param eventParam
   * @return
   * @throws Exception
   */
  public long queryEventCountCondition(String deviceId, EventParam eventParam) throws Exception {
    return queryEventCountCondition(
        deviceId, eventParam, eventParam.getTimeRangeStart(), eventParam.getTimeRangeEnd());
  }

  /**
   * 如果传入了开始结束时间,这使用该方法
   * @param deviceId
   * @param eventParam
   * @param timeRangeStart
   * @param segmentPoint
   * @return
   * @throws Exception
   */
  public long queryEventCountCondition(
      String deviceId, EventParam eventParam, long timeRangeStart, long segmentPoint)
      throws Exception {
    log.debug("收到一个ck查询参数,devideId={},eventParam={}", deviceId, eventParam);
    PreparedStatement pst = ckConn.prepareStatement(eventParam.getSql());
    pst.setString(1, deviceId);
    pst.setLong(2, timeRangeStart);
    pst.setLong(3, segmentPoint);
    ResultSet resultSet = pst.executeQuery();
    long result = 0;
    while (resultSet.next()) {
      result = resultSet.getLong("cnt");
    }
    return result;
  }

  public int queryEventSequence(String deviceId, EventSequenceParam sequenceParam)
          throws Exception {
    return queryEventSequence(deviceId, sequenceParam, sequenceParam.getTimeRangeStart(), sequenceParam.getTimeRangeEnd());

  }

  public int queryEventSequence(String deviceId, EventSequenceParam eventSequenceParam, long timeRangeStart, long segmentPoint) throws Exception {
    PreparedStatement pst = ckConn.prepareStatement(eventSequenceParam.getSql());
    pst.setString(1, deviceId);
    pst.setLong(2, timeRangeStart);
    pst.setLong(3, segmentPoint);
    ResultSet resultSet = pst.executeQuery();
    resultSet.next();
    int maxStep = 0;
    for (int i = 1; i < eventSequenceParam.getEventSequence().size(); i++) {
      // 从左边取查询结果中的字段
      // 如果取到的字段值为1，说明这个步骤被匹配了，且这就是最大匹配
      if (resultSet.getInt(i) == 1) {
        // 根据i和最大匹配步骤之间的关系，得到最大步骡数
        maxStep = eventSequenceParam.getEventSequence().size() - (i - 1);
        break;
      }
    }
    return maxStep;
  }
}
