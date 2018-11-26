package com.sixku.canal.mybatis.domain;

import java.io.Serializable;
import java.util.Date;

/**
 * @author 
 */
public class CustomerInfo implements Serializable {
    private Long id;

    /**
     * 用户ID
     */
    private String customerId;

    /**
     * 注册渠道
     */
    private String registerChannel;

    /**
     * 用户证件号码
     */
    private String certId;

    /**
     * 用户真实姓名
     */
    private String realName;

    /**
     * 用户手机号
     */
    private String mobile;

    /**
     * 户籍地址
     */
    private String nativePlace;

    /**
     * 性别(1:男,2:女)
     */
    private String sex;

    /**
     * 年龄
     */
    private Integer age;

    /**
     * 电子邮箱
     */
    private String email;

    /**
     * 视频面签号码
     */
    private String faceVideoId;

    /**
     * 登录密码
     */
    private String password;

    /**
     * 头像URL
     */
    private String headUrl;

    /**
     * 身份证正面照URL
     */
    private String certFrontUrl;

    /**
     * 身份证反面照URL
     */
    private String certReverseUrl;

    /**
     * 身份证有效期开始时间
     */
    private Date certStartDate;

    /**
     * 身份证有效期结束时间
     */
    private Date certEndDate;

    /**
     * 自己的邀请码
     */
    private String inviteCode;

    /**
     * 邀请人
     */
    private String inviteCustomer;

    /**
     * 是否实名(Y/N)
     */
    private String isRealname;

    /**
     * 网信H5页面采集标签(Y/N)
     */
    private String isWxCollect;

    /**
     * 核心用户ID
     */
    private String customerIdEarth;

    /**
     * 状态(0:启用,1:禁用)
     */
    private String status;

    /**
     * 黑名单标识(0:正常,1:黑名单)
     */
    private String blackFlag;

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

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getRegisterChannel() {
        return registerChannel;
    }

    public void setRegisterChannel(String registerChannel) {
        this.registerChannel = registerChannel;
    }

    public String getCertId() {
        return certId;
    }

    public void setCertId(String certId) {
        this.certId = certId;
    }

    public String getRealName() {
        return realName;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getNativePlace() {
        return nativePlace;
    }

    public void setNativePlace(String nativePlace) {
        this.nativePlace = nativePlace;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getFaceVideoId() {
        return faceVideoId;
    }

    public void setFaceVideoId(String faceVideoId) {
        this.faceVideoId = faceVideoId;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHeadUrl() {
        return headUrl;
    }

    public void setHeadUrl(String headUrl) {
        this.headUrl = headUrl;
    }

    public String getCertFrontUrl() {
        return certFrontUrl;
    }

    public void setCertFrontUrl(String certFrontUrl) {
        this.certFrontUrl = certFrontUrl;
    }

    public String getCertReverseUrl() {
        return certReverseUrl;
    }

    public void setCertReverseUrl(String certReverseUrl) {
        this.certReverseUrl = certReverseUrl;
    }

    public Date getCertStartDate() {
        return certStartDate;
    }

    public void setCertStartDate(Date certStartDate) {
        this.certStartDate = certStartDate;
    }

    public Date getCertEndDate() {
        return certEndDate;
    }

    public void setCertEndDate(Date certEndDate) {
        this.certEndDate = certEndDate;
    }

    public String getInviteCode() {
        return inviteCode;
    }

    public void setInviteCode(String inviteCode) {
        this.inviteCode = inviteCode;
    }

    public String getInviteCustomer() {
        return inviteCustomer;
    }

    public void setInviteCustomer(String inviteCustomer) {
        this.inviteCustomer = inviteCustomer;
    }

    public String getIsRealname() {
        return isRealname;
    }

    public void setIsRealname(String isRealname) {
        this.isRealname = isRealname;
    }

    public String getIsWxCollect() {
        return isWxCollect;
    }

    public void setIsWxCollect(String isWxCollect) {
        this.isWxCollect = isWxCollect;
    }

    public String getCustomerIdEarth() {
        return customerIdEarth;
    }

    public void setCustomerIdEarth(String customerIdEarth) {
        this.customerIdEarth = customerIdEarth;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBlackFlag() {
        return blackFlag;
    }

    public void setBlackFlag(String blackFlag) {
        this.blackFlag = blackFlag;
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
        CustomerInfo other = (CustomerInfo) that;
        return (this.getId() == null ? other.getId() == null : this.getId().equals(other.getId()))
            && (this.getCustomerId() == null ? other.getCustomerId() == null : this.getCustomerId().equals(other.getCustomerId()))
            && (this.getRegisterChannel() == null ? other.getRegisterChannel() == null : this.getRegisterChannel().equals(other.getRegisterChannel()))
            && (this.getCertId() == null ? other.getCertId() == null : this.getCertId().equals(other.getCertId()))
            && (this.getRealName() == null ? other.getRealName() == null : this.getRealName().equals(other.getRealName()))
            && (this.getMobile() == null ? other.getMobile() == null : this.getMobile().equals(other.getMobile()))
            && (this.getNativePlace() == null ? other.getNativePlace() == null : this.getNativePlace().equals(other.getNativePlace()))
            && (this.getSex() == null ? other.getSex() == null : this.getSex().equals(other.getSex()))
            && (this.getAge() == null ? other.getAge() == null : this.getAge().equals(other.getAge()))
            && (this.getEmail() == null ? other.getEmail() == null : this.getEmail().equals(other.getEmail()))
            && (this.getFaceVideoId() == null ? other.getFaceVideoId() == null : this.getFaceVideoId().equals(other.getFaceVideoId()))
            && (this.getPassword() == null ? other.getPassword() == null : this.getPassword().equals(other.getPassword()))
            && (this.getHeadUrl() == null ? other.getHeadUrl() == null : this.getHeadUrl().equals(other.getHeadUrl()))
            && (this.getCertFrontUrl() == null ? other.getCertFrontUrl() == null : this.getCertFrontUrl().equals(other.getCertFrontUrl()))
            && (this.getCertReverseUrl() == null ? other.getCertReverseUrl() == null : this.getCertReverseUrl().equals(other.getCertReverseUrl()))
            && (this.getCertStartDate() == null ? other.getCertStartDate() == null : this.getCertStartDate().equals(other.getCertStartDate()))
            && (this.getCertEndDate() == null ? other.getCertEndDate() == null : this.getCertEndDate().equals(other.getCertEndDate()))
            && (this.getInviteCode() == null ? other.getInviteCode() == null : this.getInviteCode().equals(other.getInviteCode()))
            && (this.getInviteCustomer() == null ? other.getInviteCustomer() == null : this.getInviteCustomer().equals(other.getInviteCustomer()))
            && (this.getIsRealname() == null ? other.getIsRealname() == null : this.getIsRealname().equals(other.getIsRealname()))
            && (this.getIsWxCollect() == null ? other.getIsWxCollect() == null : this.getIsWxCollect().equals(other.getIsWxCollect()))
            && (this.getCustomerIdEarth() == null ? other.getCustomerIdEarth() == null : this.getCustomerIdEarth().equals(other.getCustomerIdEarth()))
            && (this.getStatus() == null ? other.getStatus() == null : this.getStatus().equals(other.getStatus()))
            && (this.getBlackFlag() == null ? other.getBlackFlag() == null : this.getBlackFlag().equals(other.getBlackFlag()))
            && (this.getCreateUser() == null ? other.getCreateUser() == null : this.getCreateUser().equals(other.getCreateUser()))
            && (this.getUpdateUser() == null ? other.getUpdateUser() == null : this.getUpdateUser().equals(other.getUpdateUser()))
            && (this.getCreateTime() == null ? other.getCreateTime() == null : this.getCreateTime().equals(other.getCreateTime()))
            && (this.getUpdateTime() == null ? other.getUpdateTime() == null : this.getUpdateTime().equals(other.getUpdateTime()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        result = prime * result + ((getCustomerId() == null) ? 0 : getCustomerId().hashCode());
        result = prime * result + ((getRegisterChannel() == null) ? 0 : getRegisterChannel().hashCode());
        result = prime * result + ((getCertId() == null) ? 0 : getCertId().hashCode());
        result = prime * result + ((getRealName() == null) ? 0 : getRealName().hashCode());
        result = prime * result + ((getMobile() == null) ? 0 : getMobile().hashCode());
        result = prime * result + ((getNativePlace() == null) ? 0 : getNativePlace().hashCode());
        result = prime * result + ((getSex() == null) ? 0 : getSex().hashCode());
        result = prime * result + ((getAge() == null) ? 0 : getAge().hashCode());
        result = prime * result + ((getEmail() == null) ? 0 : getEmail().hashCode());
        result = prime * result + ((getFaceVideoId() == null) ? 0 : getFaceVideoId().hashCode());
        result = prime * result + ((getPassword() == null) ? 0 : getPassword().hashCode());
        result = prime * result + ((getHeadUrl() == null) ? 0 : getHeadUrl().hashCode());
        result = prime * result + ((getCertFrontUrl() == null) ? 0 : getCertFrontUrl().hashCode());
        result = prime * result + ((getCertReverseUrl() == null) ? 0 : getCertReverseUrl().hashCode());
        result = prime * result + ((getCertStartDate() == null) ? 0 : getCertStartDate().hashCode());
        result = prime * result + ((getCertEndDate() == null) ? 0 : getCertEndDate().hashCode());
        result = prime * result + ((getInviteCode() == null) ? 0 : getInviteCode().hashCode());
        result = prime * result + ((getInviteCustomer() == null) ? 0 : getInviteCustomer().hashCode());
        result = prime * result + ((getIsRealname() == null) ? 0 : getIsRealname().hashCode());
        result = prime * result + ((getIsWxCollect() == null) ? 0 : getIsWxCollect().hashCode());
        result = prime * result + ((getCustomerIdEarth() == null) ? 0 : getCustomerIdEarth().hashCode());
        result = prime * result + ((getStatus() == null) ? 0 : getStatus().hashCode());
        result = prime * result + ((getBlackFlag() == null) ? 0 : getBlackFlag().hashCode());
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
        sb.append(", id=").append(id);
        sb.append(", customerId=").append(customerId);
        sb.append(", registerChannel=").append(registerChannel);
        sb.append(", certId=").append(certId);
        sb.append(", realName=").append(realName);
        sb.append(", mobile=").append(mobile);
        sb.append(", nativePlace=").append(nativePlace);
        sb.append(", sex=").append(sex);
        sb.append(", age=").append(age);
        sb.append(", email=").append(email);
        sb.append(", faceVideoId=").append(faceVideoId);
        sb.append(", password=").append(password);
        sb.append(", headUrl=").append(headUrl);
        sb.append(", certFrontUrl=").append(certFrontUrl);
        sb.append(", certReverseUrl=").append(certReverseUrl);
        sb.append(", certStartDate=").append(certStartDate);
        sb.append(", certEndDate=").append(certEndDate);
        sb.append(", inviteCode=").append(inviteCode);
        sb.append(", inviteCustomer=").append(inviteCustomer);
        sb.append(", isRealname=").append(isRealname);
        sb.append(", isWxCollect=").append(isWxCollect);
        sb.append(", customerIdEarth=").append(customerIdEarth);
        sb.append(", status=").append(status);
        sb.append(", blackFlag=").append(blackFlag);
        sb.append(", createUser=").append(createUser);
        sb.append(", updateUser=").append(updateUser);
        sb.append(", createTime=").append(createTime);
        sb.append(", updateTime=").append(updateTime);
        sb.append(", serialVersionUID=").append(serialVersionUID);
        sb.append("]");
        return sb.toString();
    }
}