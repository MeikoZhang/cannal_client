package com.sixku.canal.mybatis.domain;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

/**
 * @author 
 */
public class OrderInfo implements Serializable {
    private Long id;

    /**
     * 订单号
     */
    private String orderId;

    /**
     * 下单渠道
     */
    private String orderChannel;

    /**
     * 渠道订单号
     */
    private String channelOrderNo;

    /**
     * 上标渠道
     */
    private String channel;

    /**
     * 第三方订单号
     */
    private String thirdNo;

    /**
     * 用户ID
     */
    private String customerId;

    /**
     * 用户姓名
     */
    private String realName;

    /**
     * 身份证号
     */
    private String certId;

    /**
     * 银行卡号
     */
    private String bankCard;

    /**
     * 银行名称
     */
    private String bankName;

    /**
     * 银行简称
     */
    private String bankCode;

    /**
     * 预留手机号
     */
    private String bankMobile;

    /**
     * 申请金额(单位:元)
     */
    private BigDecimal applySum;

    /**
     * 审批金额(单位:元)
     */
    private BigDecimal approveSum;

    /**
     * 金融产品编号
     */
    private String businessType;

    /**
     * 期数
     */
    private Integer periods;

    /**
     * 期数单位
     */
    private String periodsUnit;

    /**
     * 上标金额(单位:元)
     */
    private BigDecimal putoutSum;

    /**
     * 落单所在省份
     */
    private String province;

    /**
     * 落单所在城市
     */
    private String city;

    /**
     * 人脸识别第三方标识
     */
    private String faceFlag;

    /**
     * 人脸识别图片URL
     */
    private String faceUrl;

    /**
     * 订单状态(00:申请中,90:申请中超时关单,01:审核中,02:审核不通过,03:视频面签,93:视频面签超时关单,04:视频面签不通过,10:签约中,96:签约超时关单,20放款中,97:放款失败关单,30:放款成功,31:提现失败,32:提现中,33:提现成功,40:放款成功还款中,43:提现成功还款中,50:逾期,60:结清)
     */
    private String orderStatus;

    /**
     * 推标状态(0:队列等待,1:已上标)
     */
    private String putoutStatus;

    /**
     * 是否推送到earth(Y/N )
     */
    private String isPushEarth;

    /**
     * 流程模板
     */
    private String template;

    /**
     * crif评分
     */
    private String crifScore;

    /**
     * 同批次二次风控通过率
     */
    private BigDecimal passRate;

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

    /**
     * 是否复贷Y N
     */
    private String isRepeatLoan;

    /**
     * 是否拉起网信页面Y N
     */
    private String isOpenUrl;

    /**
     * 视频面签视频的链接
     */
    private String faceVideoUrl;

    /**
     * 扩展信息
     */
    private String extend;

    private static final long serialVersionUID = 1L;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderChannel() {
        return orderChannel;
    }

    public void setOrderChannel(String orderChannel) {
        this.orderChannel = orderChannel;
    }

    public String getChannelOrderNo() {
        return channelOrderNo;
    }

    public void setChannelOrderNo(String channelOrderNo) {
        this.channelOrderNo = channelOrderNo;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getThirdNo() {
        return thirdNo;
    }

    public void setThirdNo(String thirdNo) {
        this.thirdNo = thirdNo;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getRealName() {
        return realName;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }

    public String getCertId() {
        return certId;
    }

    public void setCertId(String certId) {
        this.certId = certId;
    }

    public String getBankCard() {
        return bankCard;
    }

    public void setBankCard(String bankCard) {
        this.bankCard = bankCard;
    }

    public String getBankName() {
        return bankName;
    }

    public void setBankName(String bankName) {
        this.bankName = bankName;
    }

    public String getBankCode() {
        return bankCode;
    }

    public void setBankCode(String bankCode) {
        this.bankCode = bankCode;
    }

    public String getBankMobile() {
        return bankMobile;
    }

    public void setBankMobile(String bankMobile) {
        this.bankMobile = bankMobile;
    }

    public BigDecimal getApplySum() {
        return applySum;
    }

    public void setApplySum(BigDecimal applySum) {
        this.applySum = applySum;
    }

    public BigDecimal getApproveSum() {
        return approveSum;
    }

    public void setApproveSum(BigDecimal approveSum) {
        this.approveSum = approveSum;
    }

    public String getBusinessType() {
        return businessType;
    }

    public void setBusinessType(String businessType) {
        this.businessType = businessType;
    }

    public Integer getPeriods() {
        return periods;
    }

    public void setPeriods(Integer periods) {
        this.periods = periods;
    }

    public String getPeriodsUnit() {
        return periodsUnit;
    }

    public void setPeriodsUnit(String periodsUnit) {
        this.periodsUnit = periodsUnit;
    }

    public BigDecimal getPutoutSum() {
        return putoutSum;
    }

    public void setPutoutSum(BigDecimal putoutSum) {
        this.putoutSum = putoutSum;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getFaceFlag() {
        return faceFlag;
    }

    public void setFaceFlag(String faceFlag) {
        this.faceFlag = faceFlag;
    }

    public String getFaceUrl() {
        return faceUrl;
    }

    public void setFaceUrl(String faceUrl) {
        this.faceUrl = faceUrl;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getPutoutStatus() {
        return putoutStatus;
    }

    public void setPutoutStatus(String putoutStatus) {
        this.putoutStatus = putoutStatus;
    }

    public String getIsPushEarth() {
        return isPushEarth;
    }

    public void setIsPushEarth(String isPushEarth) {
        this.isPushEarth = isPushEarth;
    }

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public String getCrifScore() {
        return crifScore;
    }

    public void setCrifScore(String crifScore) {
        this.crifScore = crifScore;
    }

    public BigDecimal getPassRate() {
        return passRate;
    }

    public void setPassRate(BigDecimal passRate) {
        this.passRate = passRate;
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

    public String getIsRepeatLoan() {
        return isRepeatLoan;
    }

    public void setIsRepeatLoan(String isRepeatLoan) {
        this.isRepeatLoan = isRepeatLoan;
    }

    public String getIsOpenUrl() {
        return isOpenUrl;
    }

    public void setIsOpenUrl(String isOpenUrl) {
        this.isOpenUrl = isOpenUrl;
    }

    public String getFaceVideoUrl() {
        return faceVideoUrl;
    }

    public void setFaceVideoUrl(String faceVideoUrl) {
        this.faceVideoUrl = faceVideoUrl;
    }

    public String getExtend() {
        return extend;
    }

    public void setExtend(String extend) {
        this.extend = extend;
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
        OrderInfo other = (OrderInfo) that;
        return (this.getId() == null ? other.getId() == null : this.getId().equals(other.getId()))
            && (this.getOrderId() == null ? other.getOrderId() == null : this.getOrderId().equals(other.getOrderId()))
            && (this.getOrderChannel() == null ? other.getOrderChannel() == null : this.getOrderChannel().equals(other.getOrderChannel()))
            && (this.getChannelOrderNo() == null ? other.getChannelOrderNo() == null : this.getChannelOrderNo().equals(other.getChannelOrderNo()))
            && (this.getChannel() == null ? other.getChannel() == null : this.getChannel().equals(other.getChannel()))
            && (this.getThirdNo() == null ? other.getThirdNo() == null : this.getThirdNo().equals(other.getThirdNo()))
            && (this.getCustomerId() == null ? other.getCustomerId() == null : this.getCustomerId().equals(other.getCustomerId()))
            && (this.getRealName() == null ? other.getRealName() == null : this.getRealName().equals(other.getRealName()))
            && (this.getCertId() == null ? other.getCertId() == null : this.getCertId().equals(other.getCertId()))
            && (this.getBankCard() == null ? other.getBankCard() == null : this.getBankCard().equals(other.getBankCard()))
            && (this.getBankName() == null ? other.getBankName() == null : this.getBankName().equals(other.getBankName()))
            && (this.getBankCode() == null ? other.getBankCode() == null : this.getBankCode().equals(other.getBankCode()))
            && (this.getBankMobile() == null ? other.getBankMobile() == null : this.getBankMobile().equals(other.getBankMobile()))
            && (this.getApplySum() == null ? other.getApplySum() == null : this.getApplySum().equals(other.getApplySum()))
            && (this.getApproveSum() == null ? other.getApproveSum() == null : this.getApproveSum().equals(other.getApproveSum()))
            && (this.getBusinessType() == null ? other.getBusinessType() == null : this.getBusinessType().equals(other.getBusinessType()))
            && (this.getPeriods() == null ? other.getPeriods() == null : this.getPeriods().equals(other.getPeriods()))
            && (this.getPeriodsUnit() == null ? other.getPeriodsUnit() == null : this.getPeriodsUnit().equals(other.getPeriodsUnit()))
            && (this.getPutoutSum() == null ? other.getPutoutSum() == null : this.getPutoutSum().equals(other.getPutoutSum()))
            && (this.getProvince() == null ? other.getProvince() == null : this.getProvince().equals(other.getProvince()))
            && (this.getCity() == null ? other.getCity() == null : this.getCity().equals(other.getCity()))
            && (this.getFaceFlag() == null ? other.getFaceFlag() == null : this.getFaceFlag().equals(other.getFaceFlag()))
            && (this.getFaceUrl() == null ? other.getFaceUrl() == null : this.getFaceUrl().equals(other.getFaceUrl()))
            && (this.getOrderStatus() == null ? other.getOrderStatus() == null : this.getOrderStatus().equals(other.getOrderStatus()))
            && (this.getPutoutStatus() == null ? other.getPutoutStatus() == null : this.getPutoutStatus().equals(other.getPutoutStatus()))
            && (this.getIsPushEarth() == null ? other.getIsPushEarth() == null : this.getIsPushEarth().equals(other.getIsPushEarth()))
            && (this.getTemplate() == null ? other.getTemplate() == null : this.getTemplate().equals(other.getTemplate()))
            && (this.getCrifScore() == null ? other.getCrifScore() == null : this.getCrifScore().equals(other.getCrifScore()))
            && (this.getPassRate() == null ? other.getPassRate() == null : this.getPassRate().equals(other.getPassRate()))
            && (this.getCreateUser() == null ? other.getCreateUser() == null : this.getCreateUser().equals(other.getCreateUser()))
            && (this.getUpdateUser() == null ? other.getUpdateUser() == null : this.getUpdateUser().equals(other.getUpdateUser()))
            && (this.getCreateTime() == null ? other.getCreateTime() == null : this.getCreateTime().equals(other.getCreateTime()))
            && (this.getUpdateTime() == null ? other.getUpdateTime() == null : this.getUpdateTime().equals(other.getUpdateTime()))
            && (this.getIsRepeatLoan() == null ? other.getIsRepeatLoan() == null : this.getIsRepeatLoan().equals(other.getIsRepeatLoan()))
            && (this.getIsOpenUrl() == null ? other.getIsOpenUrl() == null : this.getIsOpenUrl().equals(other.getIsOpenUrl()))
            && (this.getFaceVideoUrl() == null ? other.getFaceVideoUrl() == null : this.getFaceVideoUrl().equals(other.getFaceVideoUrl()))
            && (this.getExtend() == null ? other.getExtend() == null : this.getExtend().equals(other.getExtend()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        result = prime * result + ((getOrderId() == null) ? 0 : getOrderId().hashCode());
        result = prime * result + ((getOrderChannel() == null) ? 0 : getOrderChannel().hashCode());
        result = prime * result + ((getChannelOrderNo() == null) ? 0 : getChannelOrderNo().hashCode());
        result = prime * result + ((getChannel() == null) ? 0 : getChannel().hashCode());
        result = prime * result + ((getThirdNo() == null) ? 0 : getThirdNo().hashCode());
        result = prime * result + ((getCustomerId() == null) ? 0 : getCustomerId().hashCode());
        result = prime * result + ((getRealName() == null) ? 0 : getRealName().hashCode());
        result = prime * result + ((getCertId() == null) ? 0 : getCertId().hashCode());
        result = prime * result + ((getBankCard() == null) ? 0 : getBankCard().hashCode());
        result = prime * result + ((getBankName() == null) ? 0 : getBankName().hashCode());
        result = prime * result + ((getBankCode() == null) ? 0 : getBankCode().hashCode());
        result = prime * result + ((getBankMobile() == null) ? 0 : getBankMobile().hashCode());
        result = prime * result + ((getApplySum() == null) ? 0 : getApplySum().hashCode());
        result = prime * result + ((getApproveSum() == null) ? 0 : getApproveSum().hashCode());
        result = prime * result + ((getBusinessType() == null) ? 0 : getBusinessType().hashCode());
        result = prime * result + ((getPeriods() == null) ? 0 : getPeriods().hashCode());
        result = prime * result + ((getPeriodsUnit() == null) ? 0 : getPeriodsUnit().hashCode());
        result = prime * result + ((getPutoutSum() == null) ? 0 : getPutoutSum().hashCode());
        result = prime * result + ((getProvince() == null) ? 0 : getProvince().hashCode());
        result = prime * result + ((getCity() == null) ? 0 : getCity().hashCode());
        result = prime * result + ((getFaceFlag() == null) ? 0 : getFaceFlag().hashCode());
        result = prime * result + ((getFaceUrl() == null) ? 0 : getFaceUrl().hashCode());
        result = prime * result + ((getOrderStatus() == null) ? 0 : getOrderStatus().hashCode());
        result = prime * result + ((getPutoutStatus() == null) ? 0 : getPutoutStatus().hashCode());
        result = prime * result + ((getIsPushEarth() == null) ? 0 : getIsPushEarth().hashCode());
        result = prime * result + ((getTemplate() == null) ? 0 : getTemplate().hashCode());
        result = prime * result + ((getCrifScore() == null) ? 0 : getCrifScore().hashCode());
        result = prime * result + ((getPassRate() == null) ? 0 : getPassRate().hashCode());
        result = prime * result + ((getCreateUser() == null) ? 0 : getCreateUser().hashCode());
        result = prime * result + ((getUpdateUser() == null) ? 0 : getUpdateUser().hashCode());
        result = prime * result + ((getCreateTime() == null) ? 0 : getCreateTime().hashCode());
        result = prime * result + ((getUpdateTime() == null) ? 0 : getUpdateTime().hashCode());
        result = prime * result + ((getIsRepeatLoan() == null) ? 0 : getIsRepeatLoan().hashCode());
        result = prime * result + ((getIsOpenUrl() == null) ? 0 : getIsOpenUrl().hashCode());
        result = prime * result + ((getFaceVideoUrl() == null) ? 0 : getFaceVideoUrl().hashCode());
        result = prime * result + ((getExtend() == null) ? 0 : getExtend().hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [");
        sb.append("Hash = ").append(hashCode());
        sb.append(", id=").append(id);
        sb.append(", orderId=").append(orderId);
        sb.append(", orderChannel=").append(orderChannel);
        sb.append(", channelOrderNo=").append(channelOrderNo);
        sb.append(", channel=").append(channel);
        sb.append(", thirdNo=").append(thirdNo);
        sb.append(", customerId=").append(customerId);
        sb.append(", realName=").append(realName);
        sb.append(", certId=").append(certId);
        sb.append(", bankCard=").append(bankCard);
        sb.append(", bankName=").append(bankName);
        sb.append(", bankCode=").append(bankCode);
        sb.append(", bankMobile=").append(bankMobile);
        sb.append(", applySum=").append(applySum);
        sb.append(", approveSum=").append(approveSum);
        sb.append(", businessType=").append(businessType);
        sb.append(", periods=").append(periods);
        sb.append(", periodsUnit=").append(periodsUnit);
        sb.append(", putoutSum=").append(putoutSum);
        sb.append(", province=").append(province);
        sb.append(", city=").append(city);
        sb.append(", faceFlag=").append(faceFlag);
        sb.append(", faceUrl=").append(faceUrl);
        sb.append(", orderStatus=").append(orderStatus);
        sb.append(", putoutStatus=").append(putoutStatus);
        sb.append(", isPushEarth=").append(isPushEarth);
        sb.append(", template=").append(template);
        sb.append(", crifScore=").append(crifScore);
        sb.append(", passRate=").append(passRate);
        sb.append(", createUser=").append(createUser);
        sb.append(", updateUser=").append(updateUser);
        sb.append(", createTime=").append(createTime);
        sb.append(", updateTime=").append(updateTime);
        sb.append(", isRepeatLoan=").append(isRepeatLoan);
        sb.append(", isOpenUrl=").append(isOpenUrl);
        sb.append(", faceVideoUrl=").append(faceVideoUrl);
        sb.append(", extend=").append(extend);
        sb.append(", serialVersionUID=").append(serialVersionUID);
        sb.append("]");
        return sb.toString();
    }
}