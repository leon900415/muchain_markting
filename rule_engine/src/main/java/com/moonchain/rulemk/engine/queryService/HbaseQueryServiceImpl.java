package com.moonchain.rulemk.engine.queryService;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * queryService的实现
 *
 * @author: Moon-Chain 2022-03-01 18:33
 */
public class HbaseQueryServiceImpl implements QueryService {

  private final Connection hbaseConnection;

  public HbaseQueryServiceImpl(Connection hbaseConnection) {
    this.hbaseConnection = hbaseConnection;
  }

  public boolean queryProfileCondition(
      String deviceId,
      Map<String, String> userProfileConditions,
      String tableName,
      String familyName)
      throws IOException {

    Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
    // 设置hbase的查询条件
    Get get = new Get(deviceId.getBytes());
    Set<String> keys = userProfileConditions.keySet();
    for (String key : keys) {
      get.addColumn(familyName.getBytes(), key.getBytes());
    }
    // 请求hbase查询
    Result result = table.get(get);
    for (String key : keys) {
      byte[] value = result.getValue(familyName.getBytes(), key.getBytes());
      if (!userProfileConditions.get(key).equals(new String(value))) {
        return false;
      }
    }
    return true;
  }
}
