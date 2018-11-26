package com.sixku.canal.mybatis.domain;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CustomerInfoExample {
    protected String orderByClause;

    protected boolean distinct;

    protected List<Criteria> oredCriteria;

    private Integer limit;

    private Integer offset;

    public CustomerInfoExample() {
        oredCriteria = new ArrayList<Criteria>();
    }

    public void setOrderByClause(String orderByClause) {
        this.orderByClause = orderByClause;
    }

    public String getOrderByClause() {
        return orderByClause;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public List<Criteria> getOredCriteria() {
        return oredCriteria;
    }

    public void or(Criteria criteria) {
        oredCriteria.add(criteria);
    }

    public Criteria or() {
        Criteria criteria = createCriteriaInternal();
        oredCriteria.add(criteria);
        return criteria;
    }

    public Criteria createCriteria() {
        Criteria criteria = createCriteriaInternal();
        if (oredCriteria.size() == 0) {
            oredCriteria.add(criteria);
        }
        return criteria;
    }

    protected Criteria createCriteriaInternal() {
        Criteria criteria = new Criteria();
        return criteria;
    }

    public void clear() {
        oredCriteria.clear();
        orderByClause = null;
        distinct = false;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getOffset() {
        return offset;
    }

    protected abstract static class GeneratedCriteria {
        protected List<Criterion> criteria;

        protected GeneratedCriteria() {
            super();
            criteria = new ArrayList<Criterion>();
        }

        public boolean isValid() {
            return criteria.size() > 0;
        }

        public List<Criterion> getAllCriteria() {
            return criteria;
        }

        public List<Criterion> getCriteria() {
            return criteria;
        }

        protected void addCriterion(String condition) {
            if (condition == null) {
                throw new RuntimeException("Value for condition cannot be null");
            }
            criteria.add(new Criterion(condition));
        }

        protected void addCriterion(String condition, Object value, String property) {
            if (value == null) {
                throw new RuntimeException("Value for " + property + " cannot be null");
            }
            criteria.add(new Criterion(condition, value));
        }

        protected void addCriterion(String condition, Object value1, Object value2, String property) {
            if (value1 == null || value2 == null) {
                throw new RuntimeException("Between values for " + property + " cannot be null");
            }
            criteria.add(new Criterion(condition, value1, value2));
        }

        public Criteria andIdIsNull() {
            addCriterion("id is null");
            return (Criteria) this;
        }

        public Criteria andIdIsNotNull() {
            addCriterion("id is not null");
            return (Criteria) this;
        }

        public Criteria andIdEqualTo(Long value) {
            addCriterion("id =", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotEqualTo(Long value) {
            addCriterion("id <>", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdGreaterThan(Long value) {
            addCriterion("id >", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdGreaterThanOrEqualTo(Long value) {
            addCriterion("id >=", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdLessThan(Long value) {
            addCriterion("id <", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdLessThanOrEqualTo(Long value) {
            addCriterion("id <=", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdIn(List<Long> values) {
            addCriterion("id in", values, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotIn(List<Long> values) {
            addCriterion("id not in", values, "id");
            return (Criteria) this;
        }

        public Criteria andIdBetween(Long value1, Long value2) {
            addCriterion("id between", value1, value2, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotBetween(Long value1, Long value2) {
            addCriterion("id not between", value1, value2, "id");
            return (Criteria) this;
        }

        public Criteria andCustomerIdIsNull() {
            addCriterion("customer_id is null");
            return (Criteria) this;
        }

        public Criteria andCustomerIdIsNotNull() {
            addCriterion("customer_id is not null");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEqualTo(String value) {
            addCriterion("customer_id =", value, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdNotEqualTo(String value) {
            addCriterion("customer_id <>", value, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdGreaterThan(String value) {
            addCriterion("customer_id >", value, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdGreaterThanOrEqualTo(String value) {
            addCriterion("customer_id >=", value, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdLessThan(String value) {
            addCriterion("customer_id <", value, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdLessThanOrEqualTo(String value) {
            addCriterion("customer_id <=", value, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdLike(String value) {
            addCriterion("customer_id like", value, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdNotLike(String value) {
            addCriterion("customer_id not like", value, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdIn(List<String> values) {
            addCriterion("customer_id in", values, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdNotIn(List<String> values) {
            addCriterion("customer_id not in", values, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdBetween(String value1, String value2) {
            addCriterion("customer_id between", value1, value2, "customerId");
            return (Criteria) this;
        }

        public Criteria andCustomerIdNotBetween(String value1, String value2) {
            addCriterion("customer_id not between", value1, value2, "customerId");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelIsNull() {
            addCriterion("register_channel is null");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelIsNotNull() {
            addCriterion("register_channel is not null");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelEqualTo(String value) {
            addCriterion("register_channel =", value, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelNotEqualTo(String value) {
            addCriterion("register_channel <>", value, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelGreaterThan(String value) {
            addCriterion("register_channel >", value, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelGreaterThanOrEqualTo(String value) {
            addCriterion("register_channel >=", value, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelLessThan(String value) {
            addCriterion("register_channel <", value, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelLessThanOrEqualTo(String value) {
            addCriterion("register_channel <=", value, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelLike(String value) {
            addCriterion("register_channel like", value, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelNotLike(String value) {
            addCriterion("register_channel not like", value, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelIn(List<String> values) {
            addCriterion("register_channel in", values, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelNotIn(List<String> values) {
            addCriterion("register_channel not in", values, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelBetween(String value1, String value2) {
            addCriterion("register_channel between", value1, value2, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andRegisterChannelNotBetween(String value1, String value2) {
            addCriterion("register_channel not between", value1, value2, "registerChannel");
            return (Criteria) this;
        }

        public Criteria andCertIdIsNull() {
            addCriterion("cert_id is null");
            return (Criteria) this;
        }

        public Criteria andCertIdIsNotNull() {
            addCriterion("cert_id is not null");
            return (Criteria) this;
        }

        public Criteria andCertIdEqualTo(String value) {
            addCriterion("cert_id =", value, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdNotEqualTo(String value) {
            addCriterion("cert_id <>", value, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdGreaterThan(String value) {
            addCriterion("cert_id >", value, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdGreaterThanOrEqualTo(String value) {
            addCriterion("cert_id >=", value, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdLessThan(String value) {
            addCriterion("cert_id <", value, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdLessThanOrEqualTo(String value) {
            addCriterion("cert_id <=", value, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdLike(String value) {
            addCriterion("cert_id like", value, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdNotLike(String value) {
            addCriterion("cert_id not like", value, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdIn(List<String> values) {
            addCriterion("cert_id in", values, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdNotIn(List<String> values) {
            addCriterion("cert_id not in", values, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdBetween(String value1, String value2) {
            addCriterion("cert_id between", value1, value2, "certId");
            return (Criteria) this;
        }

        public Criteria andCertIdNotBetween(String value1, String value2) {
            addCriterion("cert_id not between", value1, value2, "certId");
            return (Criteria) this;
        }

        public Criteria andRealNameIsNull() {
            addCriterion("real_name is null");
            return (Criteria) this;
        }

        public Criteria andRealNameIsNotNull() {
            addCriterion("real_name is not null");
            return (Criteria) this;
        }

        public Criteria andRealNameEqualTo(String value) {
            addCriterion("real_name =", value, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameNotEqualTo(String value) {
            addCriterion("real_name <>", value, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameGreaterThan(String value) {
            addCriterion("real_name >", value, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameGreaterThanOrEqualTo(String value) {
            addCriterion("real_name >=", value, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameLessThan(String value) {
            addCriterion("real_name <", value, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameLessThanOrEqualTo(String value) {
            addCriterion("real_name <=", value, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameLike(String value) {
            addCriterion("real_name like", value, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameNotLike(String value) {
            addCriterion("real_name not like", value, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameIn(List<String> values) {
            addCriterion("real_name in", values, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameNotIn(List<String> values) {
            addCriterion("real_name not in", values, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameBetween(String value1, String value2) {
            addCriterion("real_name between", value1, value2, "realName");
            return (Criteria) this;
        }

        public Criteria andRealNameNotBetween(String value1, String value2) {
            addCriterion("real_name not between", value1, value2, "realName");
            return (Criteria) this;
        }

        public Criteria andMobileIsNull() {
            addCriterion("mobile is null");
            return (Criteria) this;
        }

        public Criteria andMobileIsNotNull() {
            addCriterion("mobile is not null");
            return (Criteria) this;
        }

        public Criteria andMobileEqualTo(String value) {
            addCriterion("mobile =", value, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileNotEqualTo(String value) {
            addCriterion("mobile <>", value, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileGreaterThan(String value) {
            addCriterion("mobile >", value, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileGreaterThanOrEqualTo(String value) {
            addCriterion("mobile >=", value, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileLessThan(String value) {
            addCriterion("mobile <", value, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileLessThanOrEqualTo(String value) {
            addCriterion("mobile <=", value, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileLike(String value) {
            addCriterion("mobile like", value, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileNotLike(String value) {
            addCriterion("mobile not like", value, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileIn(List<String> values) {
            addCriterion("mobile in", values, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileNotIn(List<String> values) {
            addCriterion("mobile not in", values, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileBetween(String value1, String value2) {
            addCriterion("mobile between", value1, value2, "mobile");
            return (Criteria) this;
        }

        public Criteria andMobileNotBetween(String value1, String value2) {
            addCriterion("mobile not between", value1, value2, "mobile");
            return (Criteria) this;
        }

        public Criteria andNativePlaceIsNull() {
            addCriterion("native_place is null");
            return (Criteria) this;
        }

        public Criteria andNativePlaceIsNotNull() {
            addCriterion("native_place is not null");
            return (Criteria) this;
        }

        public Criteria andNativePlaceEqualTo(String value) {
            addCriterion("native_place =", value, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceNotEqualTo(String value) {
            addCriterion("native_place <>", value, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceGreaterThan(String value) {
            addCriterion("native_place >", value, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceGreaterThanOrEqualTo(String value) {
            addCriterion("native_place >=", value, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceLessThan(String value) {
            addCriterion("native_place <", value, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceLessThanOrEqualTo(String value) {
            addCriterion("native_place <=", value, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceLike(String value) {
            addCriterion("native_place like", value, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceNotLike(String value) {
            addCriterion("native_place not like", value, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceIn(List<String> values) {
            addCriterion("native_place in", values, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceNotIn(List<String> values) {
            addCriterion("native_place not in", values, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceBetween(String value1, String value2) {
            addCriterion("native_place between", value1, value2, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andNativePlaceNotBetween(String value1, String value2) {
            addCriterion("native_place not between", value1, value2, "nativePlace");
            return (Criteria) this;
        }

        public Criteria andSexIsNull() {
            addCriterion("sex is null");
            return (Criteria) this;
        }

        public Criteria andSexIsNotNull() {
            addCriterion("sex is not null");
            return (Criteria) this;
        }

        public Criteria andSexEqualTo(String value) {
            addCriterion("sex =", value, "sex");
            return (Criteria) this;
        }

        public Criteria andSexNotEqualTo(String value) {
            addCriterion("sex <>", value, "sex");
            return (Criteria) this;
        }

        public Criteria andSexGreaterThan(String value) {
            addCriterion("sex >", value, "sex");
            return (Criteria) this;
        }

        public Criteria andSexGreaterThanOrEqualTo(String value) {
            addCriterion("sex >=", value, "sex");
            return (Criteria) this;
        }

        public Criteria andSexLessThan(String value) {
            addCriterion("sex <", value, "sex");
            return (Criteria) this;
        }

        public Criteria andSexLessThanOrEqualTo(String value) {
            addCriterion("sex <=", value, "sex");
            return (Criteria) this;
        }

        public Criteria andSexLike(String value) {
            addCriterion("sex like", value, "sex");
            return (Criteria) this;
        }

        public Criteria andSexNotLike(String value) {
            addCriterion("sex not like", value, "sex");
            return (Criteria) this;
        }

        public Criteria andSexIn(List<String> values) {
            addCriterion("sex in", values, "sex");
            return (Criteria) this;
        }

        public Criteria andSexNotIn(List<String> values) {
            addCriterion("sex not in", values, "sex");
            return (Criteria) this;
        }

        public Criteria andSexBetween(String value1, String value2) {
            addCriterion("sex between", value1, value2, "sex");
            return (Criteria) this;
        }

        public Criteria andSexNotBetween(String value1, String value2) {
            addCriterion("sex not between", value1, value2, "sex");
            return (Criteria) this;
        }

        public Criteria andAgeIsNull() {
            addCriterion("age is null");
            return (Criteria) this;
        }

        public Criteria andAgeIsNotNull() {
            addCriterion("age is not null");
            return (Criteria) this;
        }

        public Criteria andAgeEqualTo(Integer value) {
            addCriterion("age =", value, "age");
            return (Criteria) this;
        }

        public Criteria andAgeNotEqualTo(Integer value) {
            addCriterion("age <>", value, "age");
            return (Criteria) this;
        }

        public Criteria andAgeGreaterThan(Integer value) {
            addCriterion("age >", value, "age");
            return (Criteria) this;
        }

        public Criteria andAgeGreaterThanOrEqualTo(Integer value) {
            addCriterion("age >=", value, "age");
            return (Criteria) this;
        }

        public Criteria andAgeLessThan(Integer value) {
            addCriterion("age <", value, "age");
            return (Criteria) this;
        }

        public Criteria andAgeLessThanOrEqualTo(Integer value) {
            addCriterion("age <=", value, "age");
            return (Criteria) this;
        }

        public Criteria andAgeIn(List<Integer> values) {
            addCriterion("age in", values, "age");
            return (Criteria) this;
        }

        public Criteria andAgeNotIn(List<Integer> values) {
            addCriterion("age not in", values, "age");
            return (Criteria) this;
        }

        public Criteria andAgeBetween(Integer value1, Integer value2) {
            addCriterion("age between", value1, value2, "age");
            return (Criteria) this;
        }

        public Criteria andAgeNotBetween(Integer value1, Integer value2) {
            addCriterion("age not between", value1, value2, "age");
            return (Criteria) this;
        }

        public Criteria andEmailIsNull() {
            addCriterion("email is null");
            return (Criteria) this;
        }

        public Criteria andEmailIsNotNull() {
            addCriterion("email is not null");
            return (Criteria) this;
        }

        public Criteria andEmailEqualTo(String value) {
            addCriterion("email =", value, "email");
            return (Criteria) this;
        }

        public Criteria andEmailNotEqualTo(String value) {
            addCriterion("email <>", value, "email");
            return (Criteria) this;
        }

        public Criteria andEmailGreaterThan(String value) {
            addCriterion("email >", value, "email");
            return (Criteria) this;
        }

        public Criteria andEmailGreaterThanOrEqualTo(String value) {
            addCriterion("email >=", value, "email");
            return (Criteria) this;
        }

        public Criteria andEmailLessThan(String value) {
            addCriterion("email <", value, "email");
            return (Criteria) this;
        }

        public Criteria andEmailLessThanOrEqualTo(String value) {
            addCriterion("email <=", value, "email");
            return (Criteria) this;
        }

        public Criteria andEmailLike(String value) {
            addCriterion("email like", value, "email");
            return (Criteria) this;
        }

        public Criteria andEmailNotLike(String value) {
            addCriterion("email not like", value, "email");
            return (Criteria) this;
        }

        public Criteria andEmailIn(List<String> values) {
            addCriterion("email in", values, "email");
            return (Criteria) this;
        }

        public Criteria andEmailNotIn(List<String> values) {
            addCriterion("email not in", values, "email");
            return (Criteria) this;
        }

        public Criteria andEmailBetween(String value1, String value2) {
            addCriterion("email between", value1, value2, "email");
            return (Criteria) this;
        }

        public Criteria andEmailNotBetween(String value1, String value2) {
            addCriterion("email not between", value1, value2, "email");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdIsNull() {
            addCriterion("face_video_id is null");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdIsNotNull() {
            addCriterion("face_video_id is not null");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdEqualTo(String value) {
            addCriterion("face_video_id =", value, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdNotEqualTo(String value) {
            addCriterion("face_video_id <>", value, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdGreaterThan(String value) {
            addCriterion("face_video_id >", value, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdGreaterThanOrEqualTo(String value) {
            addCriterion("face_video_id >=", value, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdLessThan(String value) {
            addCriterion("face_video_id <", value, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdLessThanOrEqualTo(String value) {
            addCriterion("face_video_id <=", value, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdLike(String value) {
            addCriterion("face_video_id like", value, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdNotLike(String value) {
            addCriterion("face_video_id not like", value, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdIn(List<String> values) {
            addCriterion("face_video_id in", values, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdNotIn(List<String> values) {
            addCriterion("face_video_id not in", values, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdBetween(String value1, String value2) {
            addCriterion("face_video_id between", value1, value2, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andFaceVideoIdNotBetween(String value1, String value2) {
            addCriterion("face_video_id not between", value1, value2, "faceVideoId");
            return (Criteria) this;
        }

        public Criteria andPasswordIsNull() {
            addCriterion("password is null");
            return (Criteria) this;
        }

        public Criteria andPasswordIsNotNull() {
            addCriterion("password is not null");
            return (Criteria) this;
        }

        public Criteria andPasswordEqualTo(String value) {
            addCriterion("password =", value, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordNotEqualTo(String value) {
            addCriterion("password <>", value, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordGreaterThan(String value) {
            addCriterion("password >", value, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordGreaterThanOrEqualTo(String value) {
            addCriterion("password >=", value, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordLessThan(String value) {
            addCriterion("password <", value, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordLessThanOrEqualTo(String value) {
            addCriterion("password <=", value, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordLike(String value) {
            addCriterion("password like", value, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordNotLike(String value) {
            addCriterion("password not like", value, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordIn(List<String> values) {
            addCriterion("password in", values, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordNotIn(List<String> values) {
            addCriterion("password not in", values, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordBetween(String value1, String value2) {
            addCriterion("password between", value1, value2, "password");
            return (Criteria) this;
        }

        public Criteria andPasswordNotBetween(String value1, String value2) {
            addCriterion("password not between", value1, value2, "password");
            return (Criteria) this;
        }

        public Criteria andHeadUrlIsNull() {
            addCriterion("head_url is null");
            return (Criteria) this;
        }

        public Criteria andHeadUrlIsNotNull() {
            addCriterion("head_url is not null");
            return (Criteria) this;
        }

        public Criteria andHeadUrlEqualTo(String value) {
            addCriterion("head_url =", value, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlNotEqualTo(String value) {
            addCriterion("head_url <>", value, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlGreaterThan(String value) {
            addCriterion("head_url >", value, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlGreaterThanOrEqualTo(String value) {
            addCriterion("head_url >=", value, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlLessThan(String value) {
            addCriterion("head_url <", value, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlLessThanOrEqualTo(String value) {
            addCriterion("head_url <=", value, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlLike(String value) {
            addCriterion("head_url like", value, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlNotLike(String value) {
            addCriterion("head_url not like", value, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlIn(List<String> values) {
            addCriterion("head_url in", values, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlNotIn(List<String> values) {
            addCriterion("head_url not in", values, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlBetween(String value1, String value2) {
            addCriterion("head_url between", value1, value2, "headUrl");
            return (Criteria) this;
        }

        public Criteria andHeadUrlNotBetween(String value1, String value2) {
            addCriterion("head_url not between", value1, value2, "headUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlIsNull() {
            addCriterion("cert_front_url is null");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlIsNotNull() {
            addCriterion("cert_front_url is not null");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlEqualTo(String value) {
            addCriterion("cert_front_url =", value, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlNotEqualTo(String value) {
            addCriterion("cert_front_url <>", value, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlGreaterThan(String value) {
            addCriterion("cert_front_url >", value, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlGreaterThanOrEqualTo(String value) {
            addCriterion("cert_front_url >=", value, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlLessThan(String value) {
            addCriterion("cert_front_url <", value, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlLessThanOrEqualTo(String value) {
            addCriterion("cert_front_url <=", value, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlLike(String value) {
            addCriterion("cert_front_url like", value, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlNotLike(String value) {
            addCriterion("cert_front_url not like", value, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlIn(List<String> values) {
            addCriterion("cert_front_url in", values, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlNotIn(List<String> values) {
            addCriterion("cert_front_url not in", values, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlBetween(String value1, String value2) {
            addCriterion("cert_front_url between", value1, value2, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertFrontUrlNotBetween(String value1, String value2) {
            addCriterion("cert_front_url not between", value1, value2, "certFrontUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlIsNull() {
            addCriterion("cert_reverse_url is null");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlIsNotNull() {
            addCriterion("cert_reverse_url is not null");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlEqualTo(String value) {
            addCriterion("cert_reverse_url =", value, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlNotEqualTo(String value) {
            addCriterion("cert_reverse_url <>", value, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlGreaterThan(String value) {
            addCriterion("cert_reverse_url >", value, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlGreaterThanOrEqualTo(String value) {
            addCriterion("cert_reverse_url >=", value, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlLessThan(String value) {
            addCriterion("cert_reverse_url <", value, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlLessThanOrEqualTo(String value) {
            addCriterion("cert_reverse_url <=", value, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlLike(String value) {
            addCriterion("cert_reverse_url like", value, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlNotLike(String value) {
            addCriterion("cert_reverse_url not like", value, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlIn(List<String> values) {
            addCriterion("cert_reverse_url in", values, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlNotIn(List<String> values) {
            addCriterion("cert_reverse_url not in", values, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlBetween(String value1, String value2) {
            addCriterion("cert_reverse_url between", value1, value2, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertReverseUrlNotBetween(String value1, String value2) {
            addCriterion("cert_reverse_url not between", value1, value2, "certReverseUrl");
            return (Criteria) this;
        }

        public Criteria andCertStartDateIsNull() {
            addCriterion("cert_start_date is null");
            return (Criteria) this;
        }

        public Criteria andCertStartDateIsNotNull() {
            addCriterion("cert_start_date is not null");
            return (Criteria) this;
        }

        public Criteria andCertStartDateEqualTo(Date value) {
            addCriterion("cert_start_date =", value, "certStartDate");
            return (Criteria) this;
        }

        public Criteria andCertStartDateNotEqualTo(Date value) {
            addCriterion("cert_start_date <>", value, "certStartDate");
            return (Criteria) this;
        }

        public Criteria andCertStartDateGreaterThan(Date value) {
            addCriterion("cert_start_date >", value, "certStartDate");
            return (Criteria) this;
        }

        public Criteria andCertStartDateGreaterThanOrEqualTo(Date value) {
            addCriterion("cert_start_date >=", value, "certStartDate");
            return (Criteria) this;
        }

        public Criteria andCertStartDateLessThan(Date value) {
            addCriterion("cert_start_date <", value, "certStartDate");
            return (Criteria) this;
        }

        public Criteria andCertStartDateLessThanOrEqualTo(Date value) {
            addCriterion("cert_start_date <=", value, "certStartDate");
            return (Criteria) this;
        }

        public Criteria andCertStartDateIn(List<Date> values) {
            addCriterion("cert_start_date in", values, "certStartDate");
            return (Criteria) this;
        }

        public Criteria andCertStartDateNotIn(List<Date> values) {
            addCriterion("cert_start_date not in", values, "certStartDate");
            return (Criteria) this;
        }

        public Criteria andCertStartDateBetween(Date value1, Date value2) {
            addCriterion("cert_start_date between", value1, value2, "certStartDate");
            return (Criteria) this;
        }

        public Criteria andCertStartDateNotBetween(Date value1, Date value2) {
            addCriterion("cert_start_date not between", value1, value2, "certStartDate");
            return (Criteria) this;
        }

        public Criteria andCertEndDateIsNull() {
            addCriterion("cert_end_date is null");
            return (Criteria) this;
        }

        public Criteria andCertEndDateIsNotNull() {
            addCriterion("cert_end_date is not null");
            return (Criteria) this;
        }

        public Criteria andCertEndDateEqualTo(Date value) {
            addCriterion("cert_end_date =", value, "certEndDate");
            return (Criteria) this;
        }

        public Criteria andCertEndDateNotEqualTo(Date value) {
            addCriterion("cert_end_date <>", value, "certEndDate");
            return (Criteria) this;
        }

        public Criteria andCertEndDateGreaterThan(Date value) {
            addCriterion("cert_end_date >", value, "certEndDate");
            return (Criteria) this;
        }

        public Criteria andCertEndDateGreaterThanOrEqualTo(Date value) {
            addCriterion("cert_end_date >=", value, "certEndDate");
            return (Criteria) this;
        }

        public Criteria andCertEndDateLessThan(Date value) {
            addCriterion("cert_end_date <", value, "certEndDate");
            return (Criteria) this;
        }

        public Criteria andCertEndDateLessThanOrEqualTo(Date value) {
            addCriterion("cert_end_date <=", value, "certEndDate");
            return (Criteria) this;
        }

        public Criteria andCertEndDateIn(List<Date> values) {
            addCriterion("cert_end_date in", values, "certEndDate");
            return (Criteria) this;
        }

        public Criteria andCertEndDateNotIn(List<Date> values) {
            addCriterion("cert_end_date not in", values, "certEndDate");
            return (Criteria) this;
        }

        public Criteria andCertEndDateBetween(Date value1, Date value2) {
            addCriterion("cert_end_date between", value1, value2, "certEndDate");
            return (Criteria) this;
        }

        public Criteria andCertEndDateNotBetween(Date value1, Date value2) {
            addCriterion("cert_end_date not between", value1, value2, "certEndDate");
            return (Criteria) this;
        }

        public Criteria andInviteCodeIsNull() {
            addCriterion("invite_code is null");
            return (Criteria) this;
        }

        public Criteria andInviteCodeIsNotNull() {
            addCriterion("invite_code is not null");
            return (Criteria) this;
        }

        public Criteria andInviteCodeEqualTo(String value) {
            addCriterion("invite_code =", value, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeNotEqualTo(String value) {
            addCriterion("invite_code <>", value, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeGreaterThan(String value) {
            addCriterion("invite_code >", value, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeGreaterThanOrEqualTo(String value) {
            addCriterion("invite_code >=", value, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeLessThan(String value) {
            addCriterion("invite_code <", value, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeLessThanOrEqualTo(String value) {
            addCriterion("invite_code <=", value, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeLike(String value) {
            addCriterion("invite_code like", value, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeNotLike(String value) {
            addCriterion("invite_code not like", value, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeIn(List<String> values) {
            addCriterion("invite_code in", values, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeNotIn(List<String> values) {
            addCriterion("invite_code not in", values, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeBetween(String value1, String value2) {
            addCriterion("invite_code between", value1, value2, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCodeNotBetween(String value1, String value2) {
            addCriterion("invite_code not between", value1, value2, "inviteCode");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerIsNull() {
            addCriterion("invite_customer is null");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerIsNotNull() {
            addCriterion("invite_customer is not null");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerEqualTo(String value) {
            addCriterion("invite_customer =", value, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerNotEqualTo(String value) {
            addCriterion("invite_customer <>", value, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerGreaterThan(String value) {
            addCriterion("invite_customer >", value, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerGreaterThanOrEqualTo(String value) {
            addCriterion("invite_customer >=", value, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerLessThan(String value) {
            addCriterion("invite_customer <", value, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerLessThanOrEqualTo(String value) {
            addCriterion("invite_customer <=", value, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerLike(String value) {
            addCriterion("invite_customer like", value, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerNotLike(String value) {
            addCriterion("invite_customer not like", value, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerIn(List<String> values) {
            addCriterion("invite_customer in", values, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerNotIn(List<String> values) {
            addCriterion("invite_customer not in", values, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerBetween(String value1, String value2) {
            addCriterion("invite_customer between", value1, value2, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andInviteCustomerNotBetween(String value1, String value2) {
            addCriterion("invite_customer not between", value1, value2, "inviteCustomer");
            return (Criteria) this;
        }

        public Criteria andIsRealnameIsNull() {
            addCriterion("is_realname is null");
            return (Criteria) this;
        }

        public Criteria andIsRealnameIsNotNull() {
            addCriterion("is_realname is not null");
            return (Criteria) this;
        }

        public Criteria andIsRealnameEqualTo(String value) {
            addCriterion("is_realname =", value, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameNotEqualTo(String value) {
            addCriterion("is_realname <>", value, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameGreaterThan(String value) {
            addCriterion("is_realname >", value, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameGreaterThanOrEqualTo(String value) {
            addCriterion("is_realname >=", value, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameLessThan(String value) {
            addCriterion("is_realname <", value, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameLessThanOrEqualTo(String value) {
            addCriterion("is_realname <=", value, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameLike(String value) {
            addCriterion("is_realname like", value, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameNotLike(String value) {
            addCriterion("is_realname not like", value, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameIn(List<String> values) {
            addCriterion("is_realname in", values, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameNotIn(List<String> values) {
            addCriterion("is_realname not in", values, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameBetween(String value1, String value2) {
            addCriterion("is_realname between", value1, value2, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsRealnameNotBetween(String value1, String value2) {
            addCriterion("is_realname not between", value1, value2, "isRealname");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectIsNull() {
            addCriterion("is_wx_collect is null");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectIsNotNull() {
            addCriterion("is_wx_collect is not null");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectEqualTo(String value) {
            addCriterion("is_wx_collect =", value, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectNotEqualTo(String value) {
            addCriterion("is_wx_collect <>", value, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectGreaterThan(String value) {
            addCriterion("is_wx_collect >", value, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectGreaterThanOrEqualTo(String value) {
            addCriterion("is_wx_collect >=", value, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectLessThan(String value) {
            addCriterion("is_wx_collect <", value, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectLessThanOrEqualTo(String value) {
            addCriterion("is_wx_collect <=", value, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectLike(String value) {
            addCriterion("is_wx_collect like", value, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectNotLike(String value) {
            addCriterion("is_wx_collect not like", value, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectIn(List<String> values) {
            addCriterion("is_wx_collect in", values, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectNotIn(List<String> values) {
            addCriterion("is_wx_collect not in", values, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectBetween(String value1, String value2) {
            addCriterion("is_wx_collect between", value1, value2, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andIsWxCollectNotBetween(String value1, String value2) {
            addCriterion("is_wx_collect not between", value1, value2, "isWxCollect");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthIsNull() {
            addCriterion("customer_id_earth is null");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthIsNotNull() {
            addCriterion("customer_id_earth is not null");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthEqualTo(String value) {
            addCriterion("customer_id_earth =", value, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthNotEqualTo(String value) {
            addCriterion("customer_id_earth <>", value, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthGreaterThan(String value) {
            addCriterion("customer_id_earth >", value, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthGreaterThanOrEqualTo(String value) {
            addCriterion("customer_id_earth >=", value, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthLessThan(String value) {
            addCriterion("customer_id_earth <", value, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthLessThanOrEqualTo(String value) {
            addCriterion("customer_id_earth <=", value, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthLike(String value) {
            addCriterion("customer_id_earth like", value, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthNotLike(String value) {
            addCriterion("customer_id_earth not like", value, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthIn(List<String> values) {
            addCriterion("customer_id_earth in", values, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthNotIn(List<String> values) {
            addCriterion("customer_id_earth not in", values, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthBetween(String value1, String value2) {
            addCriterion("customer_id_earth between", value1, value2, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andCustomerIdEarthNotBetween(String value1, String value2) {
            addCriterion("customer_id_earth not between", value1, value2, "customerIdEarth");
            return (Criteria) this;
        }

        public Criteria andStatusIsNull() {
            addCriterion("status is null");
            return (Criteria) this;
        }

        public Criteria andStatusIsNotNull() {
            addCriterion("status is not null");
            return (Criteria) this;
        }

        public Criteria andStatusEqualTo(String value) {
            addCriterion("status =", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusNotEqualTo(String value) {
            addCriterion("status <>", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusGreaterThan(String value) {
            addCriterion("status >", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusGreaterThanOrEqualTo(String value) {
            addCriterion("status >=", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusLessThan(String value) {
            addCriterion("status <", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusLessThanOrEqualTo(String value) {
            addCriterion("status <=", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusLike(String value) {
            addCriterion("status like", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusNotLike(String value) {
            addCriterion("status not like", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusIn(List<String> values) {
            addCriterion("status in", values, "status");
            return (Criteria) this;
        }

        public Criteria andStatusNotIn(List<String> values) {
            addCriterion("status not in", values, "status");
            return (Criteria) this;
        }

        public Criteria andStatusBetween(String value1, String value2) {
            addCriterion("status between", value1, value2, "status");
            return (Criteria) this;
        }

        public Criteria andStatusNotBetween(String value1, String value2) {
            addCriterion("status not between", value1, value2, "status");
            return (Criteria) this;
        }

        public Criteria andBlackFlagIsNull() {
            addCriterion("black_flag is null");
            return (Criteria) this;
        }

        public Criteria andBlackFlagIsNotNull() {
            addCriterion("black_flag is not null");
            return (Criteria) this;
        }

        public Criteria andBlackFlagEqualTo(String value) {
            addCriterion("black_flag =", value, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagNotEqualTo(String value) {
            addCriterion("black_flag <>", value, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagGreaterThan(String value) {
            addCriterion("black_flag >", value, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagGreaterThanOrEqualTo(String value) {
            addCriterion("black_flag >=", value, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagLessThan(String value) {
            addCriterion("black_flag <", value, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagLessThanOrEqualTo(String value) {
            addCriterion("black_flag <=", value, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagLike(String value) {
            addCriterion("black_flag like", value, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagNotLike(String value) {
            addCriterion("black_flag not like", value, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagIn(List<String> values) {
            addCriterion("black_flag in", values, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagNotIn(List<String> values) {
            addCriterion("black_flag not in", values, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagBetween(String value1, String value2) {
            addCriterion("black_flag between", value1, value2, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andBlackFlagNotBetween(String value1, String value2) {
            addCriterion("black_flag not between", value1, value2, "blackFlag");
            return (Criteria) this;
        }

        public Criteria andCreateUserIsNull() {
            addCriterion("create_user is null");
            return (Criteria) this;
        }

        public Criteria andCreateUserIsNotNull() {
            addCriterion("create_user is not null");
            return (Criteria) this;
        }

        public Criteria andCreateUserEqualTo(String value) {
            addCriterion("create_user =", value, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserNotEqualTo(String value) {
            addCriterion("create_user <>", value, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserGreaterThan(String value) {
            addCriterion("create_user >", value, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserGreaterThanOrEqualTo(String value) {
            addCriterion("create_user >=", value, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserLessThan(String value) {
            addCriterion("create_user <", value, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserLessThanOrEqualTo(String value) {
            addCriterion("create_user <=", value, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserLike(String value) {
            addCriterion("create_user like", value, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserNotLike(String value) {
            addCriterion("create_user not like", value, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserIn(List<String> values) {
            addCriterion("create_user in", values, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserNotIn(List<String> values) {
            addCriterion("create_user not in", values, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserBetween(String value1, String value2) {
            addCriterion("create_user between", value1, value2, "createUser");
            return (Criteria) this;
        }

        public Criteria andCreateUserNotBetween(String value1, String value2) {
            addCriterion("create_user not between", value1, value2, "createUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserIsNull() {
            addCriterion("update_user is null");
            return (Criteria) this;
        }

        public Criteria andUpdateUserIsNotNull() {
            addCriterion("update_user is not null");
            return (Criteria) this;
        }

        public Criteria andUpdateUserEqualTo(String value) {
            addCriterion("update_user =", value, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserNotEqualTo(String value) {
            addCriterion("update_user <>", value, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserGreaterThan(String value) {
            addCriterion("update_user >", value, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserGreaterThanOrEqualTo(String value) {
            addCriterion("update_user >=", value, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserLessThan(String value) {
            addCriterion("update_user <", value, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserLessThanOrEqualTo(String value) {
            addCriterion("update_user <=", value, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserLike(String value) {
            addCriterion("update_user like", value, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserNotLike(String value) {
            addCriterion("update_user not like", value, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserIn(List<String> values) {
            addCriterion("update_user in", values, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserNotIn(List<String> values) {
            addCriterion("update_user not in", values, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserBetween(String value1, String value2) {
            addCriterion("update_user between", value1, value2, "updateUser");
            return (Criteria) this;
        }

        public Criteria andUpdateUserNotBetween(String value1, String value2) {
            addCriterion("update_user not between", value1, value2, "updateUser");
            return (Criteria) this;
        }

        public Criteria andCreateTimeIsNull() {
            addCriterion("create_time is null");
            return (Criteria) this;
        }

        public Criteria andCreateTimeIsNotNull() {
            addCriterion("create_time is not null");
            return (Criteria) this;
        }

        public Criteria andCreateTimeEqualTo(Date value) {
            addCriterion("create_time =", value, "createTime");
            return (Criteria) this;
        }

        public Criteria andCreateTimeNotEqualTo(Date value) {
            addCriterion("create_time <>", value, "createTime");
            return (Criteria) this;
        }

        public Criteria andCreateTimeGreaterThan(Date value) {
            addCriterion("create_time >", value, "createTime");
            return (Criteria) this;
        }

        public Criteria andCreateTimeGreaterThanOrEqualTo(Date value) {
            addCriterion("create_time >=", value, "createTime");
            return (Criteria) this;
        }

        public Criteria andCreateTimeLessThan(Date value) {
            addCriterion("create_time <", value, "createTime");
            return (Criteria) this;
        }

        public Criteria andCreateTimeLessThanOrEqualTo(Date value) {
            addCriterion("create_time <=", value, "createTime");
            return (Criteria) this;
        }

        public Criteria andCreateTimeIn(List<Date> values) {
            addCriterion("create_time in", values, "createTime");
            return (Criteria) this;
        }

        public Criteria andCreateTimeNotIn(List<Date> values) {
            addCriterion("create_time not in", values, "createTime");
            return (Criteria) this;
        }

        public Criteria andCreateTimeBetween(Date value1, Date value2) {
            addCriterion("create_time between", value1, value2, "createTime");
            return (Criteria) this;
        }

        public Criteria andCreateTimeNotBetween(Date value1, Date value2) {
            addCriterion("create_time not between", value1, value2, "createTime");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeIsNull() {
            addCriterion("update_time is null");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeIsNotNull() {
            addCriterion("update_time is not null");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeEqualTo(Date value) {
            addCriterion("update_time =", value, "updateTime");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeNotEqualTo(Date value) {
            addCriterion("update_time <>", value, "updateTime");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeGreaterThan(Date value) {
            addCriterion("update_time >", value, "updateTime");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeGreaterThanOrEqualTo(Date value) {
            addCriterion("update_time >=", value, "updateTime");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeLessThan(Date value) {
            addCriterion("update_time <", value, "updateTime");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeLessThanOrEqualTo(Date value) {
            addCriterion("update_time <=", value, "updateTime");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeIn(List<Date> values) {
            addCriterion("update_time in", values, "updateTime");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeNotIn(List<Date> values) {
            addCriterion("update_time not in", values, "updateTime");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeBetween(Date value1, Date value2) {
            addCriterion("update_time between", value1, value2, "updateTime");
            return (Criteria) this;
        }

        public Criteria andUpdateTimeNotBetween(Date value1, Date value2) {
            addCriterion("update_time not between", value1, value2, "updateTime");
            return (Criteria) this;
        }
    }

    /**
     */
    public static class Criteria extends GeneratedCriteria {

        protected Criteria() {
            super();
        }
    }

    public static class Criterion {
        private String condition;

        private Object value;

        private Object secondValue;

        private boolean noValue;

        private boolean singleValue;

        private boolean betweenValue;

        private boolean listValue;

        private String typeHandler;

        public String getCondition() {
            return condition;
        }

        public Object getValue() {
            return value;
        }

        public Object getSecondValue() {
            return secondValue;
        }

        public boolean isNoValue() {
            return noValue;
        }

        public boolean isSingleValue() {
            return singleValue;
        }

        public boolean isBetweenValue() {
            return betweenValue;
        }

        public boolean isListValue() {
            return listValue;
        }

        public String getTypeHandler() {
            return typeHandler;
        }

        protected Criterion(String condition) {
            super();
            this.condition = condition;
            this.typeHandler = null;
            this.noValue = true;
        }

        protected Criterion(String condition, Object value, String typeHandler) {
            super();
            this.condition = condition;
            this.value = value;
            this.typeHandler = typeHandler;
            if (value instanceof List<?>) {
                this.listValue = true;
            } else {
                this.singleValue = true;
            }
        }

        protected Criterion(String condition, Object value) {
            this(condition, value, null);
        }

        protected Criterion(String condition, Object value, Object secondValue, String typeHandler) {
            super();
            this.condition = condition;
            this.value = value;
            this.secondValue = secondValue;
            this.typeHandler = typeHandler;
            this.betweenValue = true;
        }

        protected Criterion(String condition, Object value, Object secondValue) {
            this(condition, value, secondValue, null);
        }
    }
}