package com.sixku.canal;

import com.alibaba.fastjson.JSON;
import com.sixku.canal.kafka.KafkaBean;
import com.sixku.canal.kafka.KafkaUtil;
import com.sixku.canal.util.MyBatisUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 发送消息类
 * 根据配置规则发送数据到kafka
 */
public class SendMessageByRule {

    protected final static Logger logger = LoggerFactory.getLogger(SendMessageByRule.class);

    //发送规则
    static Map<String,String[]> topicList = new HashMap<>();

    static{
        String monitor_tables = MyBatisUtil.getConfig("monitor_tables");
        System.out.println(monitor_tables);
        if(StringUtils.isBlank(monitor_tables)){
            throw new RuntimeException("错误！请配置监控表名称！");
        }
        String[] tables = monitor_tables.split(",");
        for(String table : tables){
            String topics_string = MyBatisUtil.getConfig(table);
            if(StringUtils.isNotBlank(topics_string)){
                topicList.put(table, topics_string.split(","));
            }
        }

    }

    public static void sendTopicByRule(KafkaBean kafkaBean){
        if(StringUtils.isBlank(kafkaBean.getEventTable())){
            logger.error("kafkaBean table is null,please check ...");
            return;
        }
        String[] topics = topicList.get(kafkaBean.getEventTable());
        for(String topic : topics){
            KafkaUtil.produce(topic, kafkaBean.getEventTable(), kafkaBean);
        }
    }

    public static void main(String[] args) {
        System.out.println(JSON.toJSONString(topicList));
    }
}
