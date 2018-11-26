package com.sixku.canal;

import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.Date;
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
import com.sixku.canal.businessEnum.BusinessType;
import com.sixku.canal.kafka.KafkaBean;
import com.sixku.canal.kafka.KafkaUtil;
import com.sixku.canal.mybatis.CustomerInfoMapper;
import com.sixku.canal.mybatis.domain.CustomerInfo;
import com.sixku.canal.mybatis.domain.OrderInfo;
import com.sixku.canal.mybatis.domain.TestCanal;
import com.sixku.canal.util.MyBatisUtil;
import com.sixku.canal.util.RowDataUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.sixku.canal.util.MyBatisUtil.getSqlSession;

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
            String tableName = entry.getHeader().getTableName();

            logger.debug(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (RowData rowData : rowChage.getRowDatasList()) {

                if (eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                    try {
                        extractFromRowData(tableName, "delete", rowData);
                    } catch (Exception e) {
                        e.printStackTrace();
                        errorColumn(rowData.getBeforeColumnsList());
                    }

                } else if (eventType == EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                    try {
                        extractFromRowData(tableName, "insert", rowData);
                    } catch (Exception e) {
                        e.printStackTrace();
                        errorColumn(rowData.getBeforeColumnsList());
                    }

                } else if (eventType == EventType.UPDATE) {
                    printColumn(rowData.getAfterColumnsList());
                    try {
                        extractFromRowData(tableName, "update", rowData);
                    } catch (Exception e) {
                        e.printStackTrace();
                        errorColumn(rowData.getBeforeColumnsList());
                    }

                }else {
                    logger.debug("event type-------> "+ eventType);
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


    private static void errorColumn(List<Column> columns) {
        for (Column column : columns) {
            logger.error(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }


    /**
     * 从rowdata中解析出kafka发送消息对象
     * @return
     */
    private static KafkaBean extractFromRowData(String tableName, String eventType, RowData rowData) throws Exception {

        KafkaBean kafkaBean = new KafkaBean();
        kafkaBean.setEventTable(tableName);
        kafkaBean.setEventType(eventType);
        kafkaBean.setEventTime(new Date());

        switch(tableName){
            case "customer_info":
                //注册
                if("insert".equals(eventType)){
                    CustomerInfo customerInfo = RowDataUtil.extract2Object(rowData, CustomerInfo.class,"after");
                    kafkaBean.setBusinessChannel(customerInfo.getRegisterChannel());
                    kafkaBean.setBusinessType(BusinessType.REGISTER.getName());
                    kafkaBean.setBusinessCustomerId(customerInfo.getCustomerId());
                    kafkaBean.setBusinessOrderId(null);
                    kafkaBean.setBusinessTime(customerInfo.getCreateTime());
                    kafkaBean.setBusinessMoney(null);
                    kafkaBean.setBusinessIsRepeat(null);

                    kafkaBean.setBusinessCreateTime(customerInfo.getCreateTime());
                    kafkaBean.setBusinessUpdateTime(customerInfo.getUpdateTime());
                    //发送注册消息
                    KafkaUtil.produce(kafkaBean.getBusinessType(), kafkaBean);
                }
                break;

            case "order_info":
                //申请、进件
                if("insert".equals(eventType)){
                    OrderInfo orderInfo = RowDataUtil.extract2Object(rowData, OrderInfo.class,"after");
                    kafkaBean.setBusinessChannel(null);
                    kafkaBean.setBusinessType(BusinessType.APPLY.getName());
                    kafkaBean.setBusinessCustomerId(orderInfo.getCustomerId());
                    kafkaBean.setBusinessOrderId(orderInfo.getOrderId());
                    kafkaBean.setBusinessTime(orderInfo.getCreateTime());
                    kafkaBean.setBusinessMoney(String.valueOf(orderInfo.getApplySum()));
                    kafkaBean.setBusinessIsRepeat(null);

                    kafkaBean.setBusinessCreateTime(orderInfo.getCreateTime());
                    kafkaBean.setBusinessUpdateTime(orderInfo.getUpdateTime());
                    KafkaUtil.produce(BusinessType.APPLY.getName(), kafkaBean);
                    if(!"80,81".contains(orderInfo.getOrderStatus())){
                        //进件
                        KafkaUtil.produce(BusinessType.LOAN.getName(), kafkaBean);
                    }
                }

                //审批、签约、放款
                if("update".equals(eventType)){
                    OrderInfo beforeOrderInfo = RowDataUtil.extract2Object(rowData, OrderInfo.class,"before");
                    OrderInfo afterOrderInfo = RowDataUtil.extract2Object(rowData, OrderInfo.class,"after");

                    kafkaBean.setBusinessChannel(null);
                    kafkaBean.setBusinessType(BusinessType.APPROVAL.getName());
                    kafkaBean.setBusinessCustomerId(afterOrderInfo.getCustomerId());
                    kafkaBean.setBusinessOrderId(afterOrderInfo.getOrderId());
                    kafkaBean.setBusinessTime(afterOrderInfo.getUpdateTime());
                    //是否复贷
                    kafkaBean.setBusinessIsRepeat(getCusomerFromIndinfo(afterOrderInfo.getCertId()));

                    kafkaBean.setBusinessCreateTime(afterOrderInfo.getCreateTime());
                    kafkaBean.setBusinessUpdateTime(afterOrderInfo.getUpdateTime());

                    //审批通过
                    if("10".equals(afterOrderInfo.getOrderStatus()) && !"10".equals(beforeOrderInfo.getOrderStatus())){
                        kafkaBean.setBusinessMoney(String.valueOf(afterOrderInfo.getApproveSum()));
                        KafkaUtil.produce(BusinessType.APPROVAL.getName(), kafkaBean);
                    }

                    //签约
                    if("20".equals(afterOrderInfo.getOrderStatus()) && !"20".equals(beforeOrderInfo.getOrderStatus())){
                        kafkaBean.setBusinessMoney(String.valueOf(afterOrderInfo.getApproveSum()));
                        KafkaUtil.produce(BusinessType.CONTRACT.getName(), kafkaBean);
                    }

                    //放款成功
                    if("30,33".contains(afterOrderInfo.getOrderStatus()) && !"30,33".contains(beforeOrderInfo.getOrderStatus())){
                        kafkaBean.setBusinessMoney(String.valueOf(afterOrderInfo.getApproveSum()));
                        KafkaUtil.produce(BusinessType.PUTOUT.getName(), kafkaBean);
                    }
                }
                break;

            default:
                break;
        }
        return kafkaBean;
    }


    /**
     * 从ind_info中查询是否是复贷人员
     * @param certid
     * @return
     */
    private static String getCusomerFromIndinfo(String certid){
        if(StringUtils.isBlank(certid)){
            return null;
        }

        SqlSession sqlSession = getSqlSession();
        CustomerInfoMapper mapper = sqlSession.getMapper(CustomerInfoMapper.class);
        String cusomerFromIndinfo = mapper.selectIndinfo(certid);

        //复贷人员判断
        if(StringUtils.isBlank(cusomerFromIndinfo)){
            return "0";
        }else{
            return "1";
        }
    }

}