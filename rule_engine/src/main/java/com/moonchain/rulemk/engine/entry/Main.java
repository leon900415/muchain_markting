package com.moonchain.rulemk.engine.entry;

import com.moonchain.rulemk.engine.beans.EventBean;
import com.moonchain.rulemk.engine.beans.RuleMatchResult;
import com.moonchain.rulemk.engine.function.Json2LogBeanMapFunction;
import com.moonchain.rulemk.engine.function.KafkaSourceBuilder;
import com.moonchain.rulemk.engine.function.RuleMatchKeyedProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

/**
 * flink的处理函数
 *
 * @author: Moon-Chain 2022-02-28 10:13
 */
public class Main {
  public static void main(String[] args) throws Exception {
    //
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    env.setParallelism(1);

    KeyedStream<EventBean, String> keyed = env.addSource(new KafkaSourceBuilder().build("zenniu_applog"))
            .map(new Json2LogBeanMapFunction())
            .filter(Objects::nonNull)
            .keyBy(EventBean::getDeviceId);

    DataStream<RuleMatchResult> matchResultDs = keyed.process(new RuleMatchKeyedProcessFunction());
    matchResultDs.print();
    env.execute();
  }
}
