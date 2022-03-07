package com.moonchain.rulemk.engine.function;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * kafka的source构建工具
 *
 * @author: Moon-Chain 2022-02-28 09:14
 */
public class KafkaSourceBuilder {

  Config config;

  public KafkaSourceBuilder() {
    config = ConfigFactory.load();
  }

  public FlinkKafkaConsumer<String> build(String topic) {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", config.getString("kafka.bootstrap.servers"));
    properties.setProperty("auto.offset.reset", config.getString("kafka.auto.offset.reset"));
    return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
  }
}
