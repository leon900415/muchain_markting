package com.moonchain.rulemk.datagen;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 数据创建类(one)
 *
 * @author: Moon-Chain 2022-02-22 16:23
 *
 **/
public class ActionLogGenOne {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "47.111.142.247:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        LogBean logBean = new LogBean();
        logBean.setDeviceId("000053");
        logBean.setEventId("E");
        Map<String, String> ps = new HashMap();
        props.put("p1", "v1");
        logBean.setProperties(ps);
        logBean.setTimeStamp(System.currentTimeMillis());

        String log = JSON.toJSONString(logBean);
        ProducerRecord<String, String> record = new ProducerRecord<>("zenniu_applog", log);
        kafkaProducer.send(record);
        kafkaProducer.flush();
    }
}
