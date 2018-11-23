package com.sixku.canal.kafka;

import com.alibaba.fastjson.JSON;
import com.sixku.canal.util.MyBatisUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * kafka工具类
 * 发送、接收消息
 */
public class KafkaUtil {

    static Producer<String, String> producer;

    static{
        Properties props = new Properties();
        props.put("bootstrap.servers", MyBatisUtil.getConfig("kafka.bootstrap.servers"));
        props.put("acks", MyBatisUtil.getConfig("kafka.acks"));
        props.put("retries", MyBatisUtil.getIntegerConfig("kafka.retries"));
        props.put("batch.size", MyBatisUtil.getIntegerConfig("kafka.batch.size"));
        props.put("linger.ms", MyBatisUtil.getIntegerConfig("kafka.linger.ms"));
        props.put("buffer.memory", MyBatisUtil.getIntegerConfig("kafka.buffer.memory"));
        props.put("key.serializer", MyBatisUtil.getConfig("kafka.key.serializer"));
        props.put("value.serializer", MyBatisUtil.getConfig("kafka.value.serializer"));

        producer = new KafkaProducer<>(props);
    }

    public static void produce(String topic, String tableName, String value){
        producer.send(new ProducerRecord<String, String>(topic, tableName, value));
    }

    public static void produce(String topic, String tableName, KafkaBean kafkaBean){
        producer.send(new ProducerRecord<String, String>(topic, tableName, JSON.toJSONString(kafkaBean)));
    }

    public static void produce(String topic, KafkaBean kafkaBean){
        producer.send(new ProducerRecord<String, String>(topic, JSON.toJSONString(kafkaBean)));
    }

    public static void  close(){
        producer.close();
    }
}
