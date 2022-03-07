package com.moonchain.rulemk.engine.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.sql.SQLException;

/**
 * 连接器工具类
 *
 * @author: Moon-Chain 2022-02-28 18:54
 */
@Slf4j
public class ConnectionUtils {

  static Config config = ConfigFactory.load();

  /*
  * 获取hbase的连接
  * */
  public static Connection getHbaseConnection() throws IOException {
    log.debug("开始创建hbase连接");
    Configuration hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set(
        ConfigNames.HBASE_ZOOKEEPER_QUORUM, config.getString(ConfigNames.HBASE_ZOOKEEPER_QUORUM));
    hbaseConfig.set(
        ConfigNames.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT,
        config.getString(ConfigNames.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT));
    log.debug("hbase链接被创建");
    return ConnectionFactory.createConnection(hbaseConfig);
  }


  /*
  * 获取clickhouse的连接
  * */
  public static ClickHouseConnection getClickhouserConnection() throws SQLException {
    log.debug("开始创建clickhouse连接");
    ClickHouseProperties properties=new ClickHouseProperties();
    properties.setUser(config.getString(ConfigNames.CLICKHOUSE_USER));
    properties.setPassword(config.getString(ConfigNames.CLICKHOUSE_PASSWORD));
    properties.setDatabase(config.getString(ConfigNames.CLICKHOUSE_DATABASE));
    ClickHouseDataSource dataSource = new ClickHouseDataSource(config.getString(ConfigNames.CLICKHOUSE_URL),properties);
    ClickHouseConnection connection = dataSource.getConnection();
    log.debug("clickhouse链接被创建");
    return connection;
  }
}
