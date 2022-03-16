package com.moonchain.rulemk.marketing.dao;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.util.Map;
import java.util.Set;

/**
 * hbase的查询服务类
 *
 * @author: Moon-Chain 2022-03-07 19:39
 */
public class HbaseQuerier {

    Connection hbaseConnection;
    Table table;
    String familyName;

    public HbaseQuerier(Connection hbaseConnection, String profileTableName, String familyName) throws Exception {
        this.hbaseConnection = hbaseConnection;
        table = hbaseConnection.getTable(TableName.valueOf((profileTableName)));
        this.familyName = familyName;
    }

    /**
     * hbase针对用户画像的条件匹配查询
     *
     * @param profileCondition 用户画像数据
     * @param deviceId         事件id
     * @return 是否匹配
     * @throws Exception 异常
     */
    public boolean queryProfileConditionIsMatch(Map<String, String> profileCondition, String deviceId) throws Exception {
        Set<String> keys = profileCondition.keySet();
        Get get = new Get(deviceId.getBytes());
        for (String key : keys) {
            get.addColumn(familyName.getBytes(), key.getBytes());
        }
        Result result = table.get(get);
        for (String key : keys) {
            String value = new String(result.getValue(familyName.getBytes(), key.getBytes()));
            if (StringUtils.isNotBlank(value) || !profileCondition.get(key).equals(value)) {
                return false;
            }
        }
        return true;
    }
}
