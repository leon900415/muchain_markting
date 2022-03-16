package com.moonchain.rulemk.marketing.utils;

/**
 * 配置类字段
 *
 * @author: Moon-Chain 2022-02-27 16:16
 */
public class ConfigNames {

  /** kafka */
  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

  public static final String KAFKA_AUTO_OFFSET_RESET = "kafka.auto.offset.reset";

  /** hbase */
  public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

  public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT =
      "hbase.zookeeper.property.clientPort";

  public static final String HBASE_TABLE_NAME = "hbase.table.name";

  public static final String HBASE_COLUMN_FAMILY_NAME = "hbase.column.family.name";

  /** clickhouse */
  public static final String CLICKHOUSE_USER = "clickhouse.user";

  public static final String CLICKHOUSE_PASSWORD = "clickhouse.password";

  public static final String CLICKHOUSE_URL = "clickhouse.url";

  public static final String CLICKHOUSE_DATABASE = "clickhouse.database";

  /** redis */
  public static final String REDIS_HOST = "redis.host";

  public static final String REDIS_PORT = "redis.port";

  public static final String REDIS_BUFFER_TTL = "redis.buffer.ttl";
}
