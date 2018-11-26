package com.sixku.canal.mybatis.domain;

import java.io.Serializable;
import java.util.Date;

/**
 * @author 
 */
public class OrderFlow implements Serializable {
    /**
     * 订单流水号
     */
    private Long flowId;

    /**
     * 订单号
     */
    private String orderId;

    /**
     * 开始状态
     */
    private String startStatus;

    /**
     * 结束状态
     */
    private String endStatus;

    /**
     * 审批结果
     */
    private String approveResult;

    /**
     * 结果说明
     */
    private String resultExplain;

    /**
     * 落单所在省份
     */
    private String province;

    /**
     * 用户注册渠道
     */
    private String registerChannel;

    /**
     * 创建人
     */
    private String createUser;

    /**
     * 修改人
     */
    private String updateUser;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 修改时间
     */
    private Date updateTime;

    private static final long serialVersionUID = 1L;

    public Long getFlowId() {
        return flowId;
    }

    public void setFlowId(Long flowId) {
        this.flowId = flowId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getStartStatus() {
        return startStatus;
    }

    public void setStartStatus(String startStatus) {
        this.startStatus = startStatus;
    }

    public String getEndStatus() {
        return endStatus;
    }

    public void setEndStatus(String endStatus) {
        this.endStatus = endStatus;
    }

    public String getApproveResult() {
        return approveResult;
    }

    public void setApproveResult(String approveResult) {
        this.approveResult = approveResult;
    }

    public String getResultExplain() {
        return resultExplain;
    }

    public void setResultExplain(String resultExplain) {
        this.resultExplain = resultExplain;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getRegisterChannel() {
        return registerChannel;
    }

    public void setRegisterChannel(String registerChannel) {
        this.registerChannel = registerChannel;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public String getUpdateUser() {
        return updateUser;
    }

    public void setUpdateUser(String updateUser) {
        this.updateUser = updateUser;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null) {
            return false;
        }
        if (getClass() != that.getClass()) {
            return false;
        }
        OrderFlow other = (OrderFlow) that;
        return (this.getFlowId() == null ? other.getFlowId() == null : this.getFlowId().equals(other.getFlowId()))
            && (this.getOrderId() == null ? other.getOrderId() == null : this.getOrderId().equals(other.getOrderId()))
            && (this.getStartStatus() == null ? other.getStartStatus() == null : this.getStartStatus().equals(other.getStartStatus()))
            && (this.getEndStatus() == null ? other.getEndStatus() == null : this.getEndStatus().equals(other.getEndStatus()))
            && (this.getApproveResult() == null ? other.getApproveResult() == null : this.getApproveResult().equals(other.getApproveResult()))
            && (this.getResultExplain() == null ? other.getResultExplain() == null : this.getResultExplain().equals(other.getResultExplain()))
            && (this.getProvince() == null ? other.getProvince() == null : this.getProvince().equals(other.getProvince()))
            && (this.getRegisterChannel() == null ? other.getRegisterChannel() == null : this.getRegisterChannel().equals(other.getRegisterChannel()))
            && (this.getCreateUser() == null ? other.getCreateUser() == null : this.getCreateUser().equals(other.getCreateUser()))
            && (this.getUpdateUser() == null ? other.getUpdateUser() == null : this.getUpdateUser().equals(other.getUpdateUser()))
            && (this.getCreateTime() == null ? other.getCreateTime() == null : this.getCreateTime().equals(other.getCreateTime()))
            && (this.getUpdateTime() == null ? other.getUpdateTime() == null : this.getUpdateTime().equals(other.getUpdateTime()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getFlowId() == null) ? 0 : getFlowId().hashCode());
        result = prime * result + ((getOrderId() == null) ? 0 : getOrderId().hashCode());
        result = prime * result + ((getStartStatus() == null) ? 0 : getStartStatus().hashCode());
        result = prime * result + ((getEndStatus() == null) ? 0 : getEndStatus().hashCode());
        result = prime * result + ((getApproveResult() == null) ? 0 : getApproveResult().hashCode());
        result = prime * result + ((getResultExplain() == null) ? 0 : getResultExplain().hashCode());
        result = prime * result + ((getProvince() == null) ? 0 : getProvince().hashCode());
        result = prime * result + ((getRegisterChannel() == null) ? 0 : getRegisterChannel().hashCode());
        result = prime * result + ((getCreateUser() == null) ? 0 : getCreateUser().hashCode());
        result = prime * result + ((getUpdateUser() == null) ? 0 : getUpdateUser().hashCode());
        result = prime * result + ((getCreateTime() == null) ? 0 : getCreateTime().hashCode());
        result = prime * result + ((getUpdateTime() == null) ? 0 : getUpdateTime().hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [");
        sb.append("Hash = ").append(hashCode());
        sb.append(", flowId=").append(flowId);
        sb.append(", orderId=").append(orderId);
        sb.append(", startStatus=").append(startStatus);
        sb.append(", endStatus=").append(endStatus);
        sb.append(", approveResult=").append(approveResult);
        sb.append(", resultExplain=").append(resultExplain);
        sb.append(", province=").append(province);
        sb.append(", registerChannel=").append(registerChannel);
        sb.append(", createUser=").append(createUser);
        sb.append(", updateUser=").append(updateUser);
        sb.append(", createTime=").append(createTime);
        sb.append(", updateTime=").append(updateTime);
        sb.append(", serialVersionUID=").append(serialVersionUID);
        sb.append("]");
        return sb.toString();
    }
}