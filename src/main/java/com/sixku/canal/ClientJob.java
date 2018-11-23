package com.sixku.canal;

import java.net.InetSocketAddress;
import java.util.List;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.sixku.canal.mybatis.domain.TestCanal;
import com.sixku.canal.util.MyBatisUtil;
import com.sixku.canal.util.RowDataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientJob {

    protected final static Logger logger = LoggerFactory.getLogger(ClientJob.class);

    public static void main(String args[]) {


        // 创建链接  
        CanalConnector connector = CanalConnectors
                .newSingleConnector(new InetSocketAddress(MyBatisUtil.getConfig("canal_address")
                                ,Integer.valueOf(MyBatisUtil.getConfig("canal_port")))
                        , MyBatisUtil.getConfig("canal_destination")
                        , MyBatisUtil.getConfig("canal_username","")
                        , MyBatisUtil.getConfig("canal_password",""));
        int batchSize = Integer.valueOf(MyBatisUtil.getConfig("canal_batchSize"));
        int emptyCount = 0;
        try {
            connector.connect();
            //订阅 监控的 数据库.表
//            connector.subscribe(".*\\..*");
            connector.subscribe(MyBatisUtil.getConfig("canal_subscribe"));
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据  
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                    }
                } else {
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    process(message.getEntries());
                }

                connector.ack(batchId); // 提交确认  
                // connector.rollback(batchId); // 处理失败, 回滚数据  
            }
        } catch (Exception e){
            logger.error("process error!", e);
        } finally {
            connector.disconnect();
            logger.info("process over!");
        }
    }

    private static void process(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            logger.debug(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                    //TODO

                } else {
                    logger.debug("-------> before");
                    printColumn(rowData.getBeforeColumnsList());
                    logger.debug("-------> after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }


    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            logger.debug(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

}