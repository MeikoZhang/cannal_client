package com.sixku.canal.kafka;

import java.util.Date;

public class KafkaBean {

    //事件发生表名
    private String eventTable;
    //事件类型 insert/update/delete
    private String eventType;
    //事件处理时间
    private Date eventTime;

    //业务渠道编号
    private String businessChannel;
    //业务类型 注册/放款...
    private String businessType;
    //业务客户id
    private String businessCustomerId;
    //业务进件编号
    private String businessOrderId;
    //业务发生时间
    private Date businessTime;
    //业务发生金额
    private String businessMoney;
    //业务客户是否复贷 0：否 1：是
    private String businessIsRepeat;
    //业务记录创建时间
    private Date businessCreateTime;
    //业务记录更新时间
    private Date businessUpdateTime;

    @Override
    public String toString() {
        return "KafkaBean{" +
                "eventTable='" + eventTable + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                ", businessChannel='" + businessChannel + '\'' +
                ", businessType='" + businessType + '\'' +
                ", businessCustomerId='" + businessCustomerId + '\'' +
                ", businessOrderId='" + businessOrderId + '\'' +
                ", businessTime=" + businessTime +
                ", businessMoney='" + businessMoney + '\'' +
                ", businessIsRepeat='" + businessIsRepeat + '\'' +
                ", businessCreateTime=" + businessCreateTime +
                ", businessUpdateTime=" + businessUpdateTime +
                '}';
    }

    public String getEventTable() {
        return eventTable;
    }

    public void setEventTable(String eventTable) {
        this.eventTable = eventTable;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    public String getBusinessType() {
        return businessType;
    }

    public void setBusinessType(String businessType) {
        this.businessType = businessType;
    }

    public String getBusinessCustomerId() {
        return businessCustomerId;
    }

    public void setBusinessCustomerId(String businessCustomerId) {
        this.businessCustomerId = businessCustomerId;
    }

    public String getBusinessOrderId() {
        return businessOrderId;
    }

    public void setBusinessOrderId(String businessOrderId) {
        this.businessOrderId = businessOrderId;
    }

    public Date getBusinessTime() {
        return businessTime;
    }

    public void setBusinessTime(Date businessTime) {
        this.businessTime = businessTime;
    }

    public String getBusinessMoney() {
        return businessMoney;
    }

    public void setBusinessMoney(String businessMoney) {
        this.businessMoney = businessMoney;
    }

    public String getBusinessIsRepeat() {
        return businessIsRepeat;
    }

    public void setBusinessIsRepeat(String businessIsRepeat) {
        this.businessIsRepeat = businessIsRepeat;
    }

    public Date getBusinessCreateTime() {
        return businessCreateTime;
    }

    public void setBusinessCreateTime(Date businessCreateTime) {
        this.businessCreateTime = businessCreateTime;
    }

    public Date getBusinessUpdateTime() {
        return businessUpdateTime;
    }

    public void setBusinessUpdateTime(Date businessUpdateTime) {
        this.businessUpdateTime = businessUpdateTime;
    }

    public String getBusinessChannel() {
        return businessChannel;
    }

    public void setBusinessChannel(String businessChannel) {
        this.businessChannel = businessChannel;
    }
}
