package com.sixku.canal.kafka;

import com.alibaba.fastjson.JSON;
import com.sixku.canal.util.MyBatisUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

/**
 * kafka工具类
 * 发送、接收消息
 */
public class KafkaUtil {

    protected final static Logger logger = LoggerFactory.getLogger(KafkaUtil.class);

    static Properties props;
    static Producer<String, String> producer;

    static{
        props = new Properties();
        props.put("bootstrap.servers", MyBatisUtil.getConfig("kafka.bootstrap.servers"));
        props.put("acks", MyBatisUtil.getConfig("kafka.acks"));
        props.put("retries", MyBatisUtil.getIntegerConfig("kafka.retries"));
        props.put("batch.size", MyBatisUtil.getIntegerConfig("kafka.batch.size"));
        props.put("linger.ms", MyBatisUtil.getIntegerConfig("kafka.linger.ms"));
        props.put("buffer.memory", MyBatisUtil.getIntegerConfig("kafka.buffer.memory"));
        props.put("key.serializer", MyBatisUtil.getConfig("kafka.key.serializer"));
        props.put("value.serializer", MyBatisUtil.getConfig("kafka.value.serializer"));

//        producer = new KafkaProducer<>(props);
    }

    public static Producer<String, String> getProducer() {
        producer = new KafkaProducer<>(props);
        return producer;
    }

    public static void produce(String topic, String value){
        getProducer();
        producer.send(new ProducerRecord<String, String>(topic, value));
        logger.info("=========>kafka send message, topic:{} ,value:{} ", topic, value);
        close();
    }

    public static void produce(String topic, String key, String value){
        getProducer();
        producer.send(new ProducerRecord<String, String>(topic, key, value));
        logger.info("=========>kafka send message, topic:%s ,value:%s ", topic, value);
        close();
    }

    public static void produce(String topic, KafkaBean kafkaBean){
        getProducer();
        producer.send(new ProducerRecord<String, String>(topic, JSON.toJSONString(kafkaBean)));
        logger.info("=========>kafka send message, topic:{} ,value:{} ", topic, JSON.toJSONString(kafkaBean));
        close();
    }

    public static void produce(String topic, String key, KafkaBean kafkaBean){
        getProducer();
        producer.send(new ProducerRecord<String, String>(topic, key, JSON.toJSONString(kafkaBean)));
        logger.info("=========>kafka send message, topic:%s ,value:%s ", topic, JSON.toJSONString(kafkaBean));
        close();
    }


    public static void  close(){
        producer.close();
    }

    public static void main(String[] args) {
        KafkaBean kafkaBean = new KafkaBean();

        kafkaBean.setEventTable("customer_info");
        kafkaBean.setEventTime(new Date());
        kafkaBean.setEventType("insert");

        kafkaBean.setBusinessChannel("channel_code");
        kafkaBean.setBusinessType("register");
        kafkaBean.setBusinessCustomerId("customer_id");
        kafkaBean.setBusinessOrderId("loan_apply_id");
        kafkaBean.setBusinessTime(new Date());
        kafkaBean.setBusinessMoney("1000");
        kafkaBean.setBusinessIsRepeat("0");

        kafkaBean.setBusinessCreateTime(new Date());
        kafkaBean.setBusinessUpdateTime(new Date());

        KafkaUtil.getProducer();
        KafkaUtil.produce("register", kafkaBean);
        KafkaUtil.close();
    }
}
