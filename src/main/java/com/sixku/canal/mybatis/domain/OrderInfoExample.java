package com.sixku.canal.mybatis.domain;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OrderInfoExample {
    protected String orderByClause;

    protected boolean distinct;

    protected List<Criteria> oredCriteria;

    private Integer limit;

    private Integer offset;

    public OrderInfoExample() {
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

        public Criteria andOrderIdIsNull() {
            addCriterion("order_id is null");
            return (Criteria) this;
        }

        public Criteria andOrderIdIsNotNull() {
            addCriterion("order_id is not null");
            return (Criteria) this;
        }

        public Criteria andOrderIdEqualTo(String value) {
            addCriterion("order_id =", value, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdNotEqualTo(String value) {
            addCriterion("order_id <>", value, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdGreaterThan(String value) {
            addCriterion("order_id >", value, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdGreaterThanOrEqualTo(String value) {
            addCriterion("order_id >=", value, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdLessThan(String value) {
            addCriterion("order_id <", value, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdLessThanOrEqualTo(String value) {
            addCriterion("order_id <=", value, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdLike(String value) {
            addCriterion("order_id like", value, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdNotLike(String value) {
            addCriterion("order_id not like", value, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdIn(List<String> values) {
            addCriterion("order_id in", values, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdNotIn(List<String> values) {
            addCriterion("order_id not in", values, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdBetween(String value1, String value2) {
            addCriterion("order_id between", value1, value2, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderIdNotBetween(String value1, String value2) {
            addCriterion("order_id not between", value1, value2, "orderId");
            return (Criteria) this;
        }

        public Criteria andOrderChannelIsNull() {
            addCriterion("order_channel is null");
            return (Criteria) this;
        }

        public Criteria andOrderChannelIsNotNull() {
            addCriterion("order_channel is not null");
            return (Criteria) this;
        }

        public Criteria andOrderChannelEqualTo(String value) {
            addCriterion("order_channel =", value, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelNotEqualTo(String value) {
            addCriterion("order_channel <>", value, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelGreaterThan(String value) {
            addCriterion("order_channel >", value, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelGreaterThanOrEqualTo(String value) {
            addCriterion("order_channel >=", value, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelLessThan(String value) {
            addCriterion("order_channel <", value, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelLessThanOrEqualTo(String value) {
            addCriterion("order_channel <=", value, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelLike(String value) {
            addCriterion("order_channel like", value, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelNotLike(String value) {
            addCriterion("order_channel not like", value, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelIn(List<String> values) {
            addCriterion("order_channel in", values, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelNotIn(List<String> values) {
            addCriterion("order_channel not in", values, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelBetween(String value1, String value2) {
            addCriterion("order_channel between", value1, value2, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andOrderChannelNotBetween(String value1, String value2) {
            addCriterion("order_channel not between", value1, value2, "orderChannel");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoIsNull() {
            addCriterion("channel_order_no is null");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoIsNotNull() {
            addCriterion("channel_order_no is not null");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoEqualTo(String value) {
            addCriterion("channel_order_no =", value, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoNotEqualTo(String value) {
            addCriterion("channel_order_no <>", value, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoGreaterThan(String value) {
            addCriterion("channel_order_no >", value, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoGreaterThanOrEqualTo(String value) {
            addCriterion("channel_order_no >=", value, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoLessThan(String value) {
            addCriterion("channel_order_no <", value, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoLessThanOrEqualTo(String value) {
            addCriterion("channel_order_no <=", value, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoLike(String value) {
            addCriterion("channel_order_no like", value, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoNotLike(String value) {
            addCriterion("channel_order_no not like", value, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoIn(List<String> values) {
            addCriterion("channel_order_no in", values, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoNotIn(List<String> values) {
            addCriterion("channel_order_no not in", values, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoBetween(String value1, String value2) {
            addCriterion("channel_order_no between", value1, value2, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelOrderNoNotBetween(String value1, String value2) {
            addCriterion("channel_order_no not between", value1, value2, "channelOrderNo");
            return (Criteria) this;
        }

        public Criteria andChannelIsNull() {
            addCriterion("channel is null");
            return (Criteria) this;
        }

        public Criteria andChannelIsNotNull() {
            addCriterion("channel is not null");
            return (Criteria) this;
        }

        public Criteria andChannelEqualTo(String value) {
            addCriterion("channel =", value, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelNotEqualTo(String value) {
            addCriterion("channel <>", value, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelGreaterThan(String value) {
            addCriterion("channel >", value, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelGreaterThanOrEqualTo(String value) {
            addCriterion("channel >=", value, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelLessThan(String value) {
            addCriterion("channel <", value, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelLessThanOrEqualTo(String value) {
            addCriterion("channel <=", value, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelLike(String value) {
            addCriterion("channel like", value, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelNotLike(String value) {
            addCriterion("channel not like", value, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelIn(List<String> values) {
            addCriterion("channel in", values, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelNotIn(List<String> values) {
            addCriterion("channel not in", values, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelBetween(String value1, String value2) {
            addCriterion("channel between", value1, value2, "channel");
            return (Criteria) this;
        }

        public Criteria andChannelNotBetween(String value1, String value2) {
            addCriterion("channel not between", value1, value2, "channel");
            return (Criteria) this;
        }

        public Criteria andThirdNoIsNull() {
            addCriterion("third_no is null");
            return (Criteria) this;
        }

        public Criteria andThirdNoIsNotNull() {
            addCriterion("third_no is not null");
            return (Criteria) this;
        }

        public Criteria andThirdNoEqualTo(String value) {
            addCriterion("third_no =", value, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoNotEqualTo(String value) {
            addCriterion("third_no <>", value, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoGreaterThan(String value) {
            addCriterion("third_no >", value, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoGreaterThanOrEqualTo(String value) {
            addCriterion("third_no >=", value, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoLessThan(String value) {
            addCriterion("third_no <", value, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoLessThanOrEqualTo(String value) {
            addCriterion("third_no <=", value, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoLike(String value) {
            addCriterion("third_no like", value, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoNotLike(String value) {
            addCriterion("third_no not like", value, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoIn(List<String> values) {
            addCriterion("third_no in", values, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoNotIn(List<String> values) {
            addCriterion("third_no not in", values, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoBetween(String value1, String value2) {
            addCriterion("third_no between", value1, value2, "thirdNo");
            return (Criteria) this;
        }

        public Criteria andThirdNoNotBetween(String value1, String value2) {
            addCriterion("third_no not between", value1, value2, "thirdNo");
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

        public Criteria andBankCardIsNull() {
            addCriterion("bank_card is null");
            return (Criteria) this;
        }

        public Criteria andBankCardIsNotNull() {
            addCriterion("bank_card is not null");
            return (Criteria) this;
        }

        public Criteria andBankCardEqualTo(String value) {
            addCriterion("bank_card =", value, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardNotEqualTo(String value) {
            addCriterion("bank_card <>", value, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardGreaterThan(String value) {
            addCriterion("bank_card >", value, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardGreaterThanOrEqualTo(String value) {
            addCriterion("bank_card >=", value, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardLessThan(String value) {
            addCriterion("bank_card <", value, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardLessThanOrEqualTo(String value) {
            addCriterion("bank_card <=", value, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardLike(String value) {
            addCriterion("bank_card like", value, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardNotLike(String value) {
            addCriterion("bank_card not like", value, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardIn(List<String> values) {
            addCriterion("bank_card in", values, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardNotIn(List<String> values) {
            addCriterion("bank_card not in", values, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardBetween(String value1, String value2) {
            addCriterion("bank_card between", value1, value2, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankCardNotBetween(String value1, String value2) {
            addCriterion("bank_card not between", value1, value2, "bankCard");
            return (Criteria) this;
        }

        public Criteria andBankNameIsNull() {
            addCriterion("bank_name is null");
            return (Criteria) this;
        }

        public Criteria andBankNameIsNotNull() {
            addCriterion("bank_name is not null");
            return (Criteria) this;
        }

        public Criteria andBankNameEqualTo(String value) {
            addCriterion("bank_name =", value, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameNotEqualTo(String value) {
            addCriterion("bank_name <>", value, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameGreaterThan(String value) {
            addCriterion("bank_name >", value, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameGreaterThanOrEqualTo(String value) {
            addCriterion("bank_name >=", value, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameLessThan(String value) {
            addCriterion("bank_name <", value, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameLessThanOrEqualTo(String value) {
            addCriterion("bank_name <=", value, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameLike(String value) {
            addCriterion("bank_name like", value, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameNotLike(String value) {
            addCriterion("bank_name not like", value, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameIn(List<String> values) {
            addCriterion("bank_name in", values, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameNotIn(List<String> values) {
            addCriterion("bank_name not in", values, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameBetween(String value1, String value2) {
            addCriterion("bank_name between", value1, value2, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankNameNotBetween(String value1, String value2) {
            addCriterion("bank_name not between", value1, value2, "bankName");
            return (Criteria) this;
        }

        public Criteria andBankCodeIsNull() {
            addCriterion("bank_code is null");
            return (Criteria) this;
        }

        public Criteria andBankCodeIsNotNull() {
            addCriterion("bank_code is not null");
            return (Criteria) this;
        }

        public Criteria andBankCodeEqualTo(String value) {
            addCriterion("bank_code =", value, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeNotEqualTo(String value) {
            addCriterion("bank_code <>", value, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeGreaterThan(String value) {
            addCriterion("bank_code >", value, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeGreaterThanOrEqualTo(String value) {
            addCriterion("bank_code >=", value, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeLessThan(String value) {
            addCriterion("bank_code <", value, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeLessThanOrEqualTo(String value) {
            addCriterion("bank_code <=", value, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeLike(String value) {
            addCriterion("bank_code like", value, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeNotLike(String value) {
            addCriterion("bank_code not like", value, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeIn(List<String> values) {
            addCriterion("bank_code in", values, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeNotIn(List<String> values) {
            addCriterion("bank_code not in", values, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeBetween(String value1, String value2) {
            addCriterion("bank_code between", value1, value2, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankCodeNotBetween(String value1, String value2) {
            addCriterion("bank_code not between", value1, value2, "bankCode");
            return (Criteria) this;
        }

        public Criteria andBankMobileIsNull() {
            addCriterion("bank_mobile is null");
            return (Criteria) this;
        }

        public Criteria andBankMobileIsNotNull() {
            addCriterion("bank_mobile is not null");
            return (Criteria) this;
        }

        public Criteria andBankMobileEqualTo(String value) {
            addCriterion("bank_mobile =", value, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileNotEqualTo(String value) {
            addCriterion("bank_mobile <>", value, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileGreaterThan(String value) {
            addCriterion("bank_mobile >", value, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileGreaterThanOrEqualTo(String value) {
            addCriterion("bank_mobile >=", value, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileLessThan(String value) {
            addCriterion("bank_mobile <", value, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileLessThanOrEqualTo(String value) {
            addCriterion("bank_mobile <=", value, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileLike(String value) {
            addCriterion("bank_mobile like", value, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileNotLike(String value) {
            addCriterion("bank_mobile not like", value, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileIn(List<String> values) {
            addCriterion("bank_mobile in", values, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileNotIn(List<String> values) {
            addCriterion("bank_mobile not in", values, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileBetween(String value1, String value2) {
            addCriterion("bank_mobile between", value1, value2, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andBankMobileNotBetween(String value1, String value2) {
            addCriterion("bank_mobile not between", value1, value2, "bankMobile");
            return (Criteria) this;
        }

        public Criteria andApplySumIsNull() {
            addCriterion("apply_sum is null");
            return (Criteria) this;
        }

        public Criteria andApplySumIsNotNull() {
            addCriterion("apply_sum is not null");
            return (Criteria) this;
        }

        public Criteria andApplySumEqualTo(BigDecimal value) {
            addCriterion("apply_sum =", value, "applySum");
            return (Criteria) this;
        }

        public Criteria andApplySumNotEqualTo(BigDecimal value) {
            addCriterion("apply_sum <>", value, "applySum");
            return (Criteria) this;
        }

        public Criteria andApplySumGreaterThan(BigDecimal value) {
            addCriterion("apply_sum >", value, "applySum");
            return (Criteria) this;
        }

        public Criteria andApplySumGreaterThanOrEqualTo(BigDecimal value) {
            addCriterion("apply_sum >=", value, "applySum");
            return (Criteria) this;
        }

        public Criteria andApplySumLessThan(BigDecimal value) {
            addCriterion("apply_sum <", value, "applySum");
            return (Criteria) this;
        }

        public Criteria andApplySumLessThanOrEqualTo(BigDecimal value) {
            addCriterion("apply_sum <=", value, "applySum");
            return (Criteria) this;
        }

        public Criteria andApplySumIn(List<BigDecimal> values) {
            addCriterion("apply_sum in", values, "applySum");
            return (Criteria) this;
        }

        public Criteria andApplySumNotIn(List<BigDecimal> values) {
            addCriterion("apply_sum not in", values, "applySum");
            return (Criteria) this;
        }

        public Criteria andApplySumBetween(BigDecimal value1, BigDecimal value2) {
            addCriterion("apply_sum between", value1, value2, "applySum");
            return (Criteria) this;
        }

        public Criteria andApplySumNotBetween(BigDecimal value1, BigDecimal value2) {
            addCriterion("apply_sum not between", value1, value2, "applySum");
            return (Criteria) this;
        }

        public Criteria andApproveSumIsNull() {
            addCriterion("approve_sum is null");
            return (Criteria) this;
        }

        public Criteria andApproveSumIsNotNull() {
            addCriterion("approve_sum is not null");
            return (Criteria) this;
        }

        public Criteria andApproveSumEqualTo(BigDecimal value) {
            addCriterion("approve_sum =", value, "approveSum");
            return (Criteria) this;
        }

        public Criteria andApproveSumNotEqualTo(BigDecimal value) {
            addCriterion("approve_sum <>", value, "approveSum");
            return (Criteria) this;
        }

        public Criteria andApproveSumGreaterThan(BigDecimal value) {
            addCriterion("approve_sum >", value, "approveSum");
            return (Criteria) this;
        }

        public Criteria andApproveSumGreaterThanOrEqualTo(BigDecimal value) {
            addCriterion("approve_sum >=", value, "approveSum");
            return (Criteria) this;
        }

        public Criteria andApproveSumLessThan(BigDecimal value) {
            addCriterion("approve_sum <", value, "approveSum");
            return (Criteria) this;
        }

        public Criteria andApproveSumLessThanOrEqualTo(BigDecimal value) {
            addCriterion("approve_sum <=", value, "approveSum");
            return (Criteria) this;
        }

        public Criteria andApproveSumIn(List<BigDecimal> values) {
            addCriterion("approve_sum in", values, "approveSum");
            return (Criteria) this;
        }

        public Criteria andApproveSumNotIn(List<BigDecimal> values) {
            addCriterion("approve_sum not in", values, "approveSum");
            return (Criteria) this;
        }

        public Criteria andApproveSumBetween(BigDecimal value1, BigDecimal value2) {
            addCriterion("approve_sum between", value1, value2, "approveSum");
            return (Criteria) this;
        }

        public Criteria andApproveSumNotBetween(BigDecimal value1, BigDecimal value2) {
            addCriterion("approve_sum not between", value1, value2, "approveSum");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeIsNull() {
            addCriterion("business_type is null");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeIsNotNull() {
            addCriterion("business_type is not null");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeEqualTo(String value) {
            addCriterion("business_type =", value, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeNotEqualTo(String value) {
            addCriterion("business_type <>", value, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeGreaterThan(String value) {
            addCriterion("business_type >", value, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeGreaterThanOrEqualTo(String value) {
            addCriterion("business_type >=", value, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeLessThan(String value) {
            addCriterion("business_type <", value, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeLessThanOrEqualTo(String value) {
            addCriterion("business_type <=", value, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeLike(String value) {
            addCriterion("business_type like", value, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeNotLike(String value) {
            addCriterion("business_type not like", value, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeIn(List<String> values) {
            addCriterion("business_type in", values, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeNotIn(List<String> values) {
            addCriterion("business_type not in", values, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeBetween(String value1, String value2) {
            addCriterion("business_type between", value1, value2, "businessType");
            return (Criteria) this;
        }

        public Criteria andBusinessTypeNotBetween(String value1, String value2) {
            addCriterion("business_type not between", value1, value2, "businessType");
            return (Criteria) this;
        }

        public Criteria andPeriodsIsNull() {
            addCriterion("periods is null");
            return (Criteria) this;
        }

        public Criteria andPeriodsIsNotNull() {
            addCriterion("periods is not null");
            return (Criteria) this;
        }

        public Criteria andPeriodsEqualTo(Integer value) {
            addCriterion("periods =", value, "periods");
            return (Criteria) this;
        }

        public Criteria andPeriodsNotEqualTo(Integer value) {
            addCriterion("periods <>", value, "periods");
            return (Criteria) this;
        }

        public Criteria andPeriodsGreaterThan(Integer value) {
            addCriterion("periods >", value, "periods");
            return (Criteria) this;
        }

        public Criteria andPeriodsGreaterThanOrEqualTo(Integer value) {
            addCriterion("periods >=", value, "periods");
            return (Criteria) this;
        }

        public Criteria andPeriodsLessThan(Integer value) {
            addCriterion("periods <", value, "periods");
            return (Criteria) this;
        }

        public Criteria andPeriodsLessThanOrEqualTo(Integer value) {
            addCriterion("periods <=", value, "periods");
            return (Criteria) this;
        }

        public Criteria andPeriodsIn(List<Integer> values) {
            addCriterion("periods in", values, "periods");
            return (Criteria) this;
        }

        public Criteria andPeriodsNotIn(List<Integer> values) {
            addCriterion("periods not in", values, "periods");
            return (Criteria) this;
        }

        public Criteria andPeriodsBetween(Integer value1, Integer value2) {
            addCriterion("periods between", value1, value2, "periods");
            return (Criteria) this;
        }

        public Criteria andPeriodsNotBetween(Integer value1, Integer value2) {
            addCriterion("periods not between", value1, value2, "periods");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitIsNull() {
            addCriterion("periods_unit is null");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitIsNotNull() {
            addCriterion("periods_unit is not null");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitEqualTo(String value) {
            addCriterion("periods_unit =", value, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitNotEqualTo(String value) {
            addCriterion("periods_unit <>", value, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitGreaterThan(String value) {
            addCriterion("periods_unit >", value, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitGreaterThanOrEqualTo(String value) {
            addCriterion("periods_unit >=", value, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitLessThan(String value) {
            addCriterion("periods_unit <", value, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitLessThanOrEqualTo(String value) {
            addCriterion("periods_unit <=", value, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitLike(String value) {
            addCriterion("periods_unit like", value, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitNotLike(String value) {
            addCriterion("periods_unit not like", value, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitIn(List<String> values) {
            addCriterion("periods_unit in", values, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitNotIn(List<String> values) {
            addCriterion("periods_unit not in", values, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitBetween(String value1, String value2) {
            addCriterion("periods_unit between", value1, value2, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPeriodsUnitNotBetween(String value1, String value2) {
            addCriterion("periods_unit not between", value1, value2, "periodsUnit");
            return (Criteria) this;
        }

        public Criteria andPutoutSumIsNull() {
            addCriterion("putout_sum is null");
            return (Criteria) this;
        }

        public Criteria andPutoutSumIsNotNull() {
            addCriterion("putout_sum is not null");
            return (Criteria) this;
        }

        public Criteria andPutoutSumEqualTo(BigDecimal value) {
            addCriterion("putout_sum =", value, "putoutSum");
            return (Criteria) this;
        }

        public Criteria andPutoutSumNotEqualTo(BigDecimal value) {
            addCriterion("putout_sum <>", value, "putoutSum");
            return (Criteria) this;
        }

        public Criteria andPutoutSumGreaterThan(BigDecimal value) {
            addCriterion("putout_sum >", value, "putoutSum");
            return (Criteria) this;
        }

        public Criteria andPutoutSumGreaterThanOrEqualTo(BigDecimal value) {
            addCriterion("putout_sum >=", value, "putoutSum");
            return (Criteria) this;
        }

        public Criteria andPutoutSumLessThan(BigDecimal value) {
            addCriterion("putout_sum <", value, "putoutSum");
            return (Criteria) this;
        }

        public Criteria andPutoutSumLessThanOrEqualTo(BigDecimal value) {
            addCriterion("putout_sum <=", value, "putoutSum");
            return (Criteria) this;
        }

        public Criteria andPutoutSumIn(List<BigDecimal> values) {
            addCriterion("putout_sum in", values, "putoutSum");
            return (Criteria) this;
        }

        public Criteria andPutoutSumNotIn(List<BigDecimal> values) {
            addCriterion("putout_sum not in", values, "putoutSum");
            return (Criteria) this;
        }

        public Criteria andPutoutSumBetween(BigDecimal value1, BigDecimal value2) {
            addCriterion("putout_sum between", value1, value2, "putoutSum");
            return (Criteria) this;
        }

        public Criteria andPutoutSumNotBetween(BigDecimal value1, BigDecimal value2) {
            addCriterion("putout_sum not between", value1, value2, "putoutSum");
            return (Criteria) this;
        }

        public Criteria andProvinceIsNull() {
            addCriterion("province is null");
            return (Criteria) this;
        }

        public Criteria andProvinceIsNotNull() {
            addCriterion("province is not null");
            return (Criteria) this;
        }

        public Criteria andProvinceEqualTo(String value) {
            addCriterion("province =", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceNotEqualTo(String value) {
            addCriterion("province <>", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceGreaterThan(String value) {
            addCriterion("province >", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceGreaterThanOrEqualTo(String value) {
            addCriterion("province >=", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceLessThan(String value) {
            addCriterion("province <", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceLessThanOrEqualTo(String value) {
            addCriterion("province <=", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceLike(String value) {
            addCriterion("province like", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceNotLike(String value) {
            addCriterion("province not like", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceIn(List<String> values) {
            addCriterion("province in", values, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceNotIn(List<String> values) {
            addCriterion("province not in", values, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceBetween(String value1, String value2) {
            addCriterion("province between", value1, value2, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceNotBetween(String value1, String value2) {
            addCriterion("province not between", value1, value2, "province");
            return (Criteria) this;
        }

        public Criteria andCityIsNull() {
            addCriterion("city is null");
            return (Criteria) this;
        }

        public Criteria andCityIsNotNull() {
            addCriterion("city is not null");
            return (Criteria) this;
        }

        public Criteria andCityEqualTo(String value) {
            addCriterion("city =", value, "city");
            return (Criteria) this;
        }

        public Criteria andCityNotEqualTo(String value) {
            addCriterion("city <>", value, "city");
            return (Criteria) this;
        }

        public Criteria andCityGreaterThan(String value) {
            addCriterion("city >", value, "city");
            return (Criteria) this;
        }

        public Criteria andCityGreaterThanOrEqualTo(String value) {
            addCriterion("city >=", value, "city");
            return (Criteria) this;
        }

        public Criteria andCityLessThan(String value) {
            addCriterion("city <", value, "city");
            return (Criteria) this;
        }

        public Criteria andCityLessThanOrEqualTo(String value) {
            addCriterion("city <=", value, "city");
            return (Criteria) this;
        }

        public Criteria andCityLike(String value) {
            addCriterion("city like", value, "city");
            return (Criteria) this;
        }

        public Criteria andCityNotLike(String value) {
            addCriterion("city not like", value, "city");
            return (Criteria) this;
        }

        public Criteria andCityIn(List<String> values) {
            addCriterion("city in", values, "city");
            return (Criteria) this;
        }

        public Criteria andCityNotIn(List<String> values) {
            addCriterion("city not in", values, "city");
            return (Criteria) this;
        }

        public Criteria andCityBetween(String value1, String value2) {
            addCriterion("city between", value1, value2, "city");
            return (Criteria) this;
        }

        public Criteria andCityNotBetween(String value1, String value2) {
            addCriterion("city not between", value1, value2, "city");
            return (Criteria) this;
        }

        public Criteria andFaceFlagIsNull() {
            addCriterion("face_flag is null");
            return (Criteria) this;
        }

        public Criteria andFaceFlagIsNotNull() {
            addCriterion("face_flag is not null");
            return (Criteria) this;
        }

        public Criteria andFaceFlagEqualTo(String value) {
            addCriterion("face_flag =", value, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagNotEqualTo(String value) {
            addCriterion("face_flag <>", value, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagGreaterThan(String value) {
            addCriterion("face_flag >", value, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagGreaterThanOrEqualTo(String value) {
            addCriterion("face_flag >=", value, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagLessThan(String value) {
            addCriterion("face_flag <", value, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagLessThanOrEqualTo(String value) {
            addCriterion("face_flag <=", value, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagLike(String value) {
            addCriterion("face_flag like", value, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagNotLike(String value) {
            addCriterion("face_flag not like", value, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagIn(List<String> values) {
            addCriterion("face_flag in", values, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagNotIn(List<String> values) {
            addCriterion("face_flag not in", values, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagBetween(String value1, String value2) {
            addCriterion("face_flag between", value1, value2, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceFlagNotBetween(String value1, String value2) {
            addCriterion("face_flag not between", value1, value2, "faceFlag");
            return (Criteria) this;
        }

        public Criteria andFaceUrlIsNull() {
            addCriterion("face_url is null");
            return (Criteria) this;
        }

        public Criteria andFaceUrlIsNotNull() {
            addCriterion("face_url is not null");
            return (Criteria) this;
        }

        public Criteria andFaceUrlEqualTo(String value) {
            addCriterion("face_url =", value, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlNotEqualTo(String value) {
            addCriterion("face_url <>", value, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlGreaterThan(String value) {
            addCriterion("face_url >", value, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlGreaterThanOrEqualTo(String value) {
            addCriterion("face_url >=", value, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlLessThan(String value) {
            addCriterion("face_url <", value, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlLessThanOrEqualTo(String value) {
            addCriterion("face_url <=", value, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlLike(String value) {
            addCriterion("face_url like", value, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlNotLike(String value) {
            addCriterion("face_url not like", value, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlIn(List<String> values) {
            addCriterion("face_url in", values, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlNotIn(List<String> values) {
            addCriterion("face_url not in", values, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlBetween(String value1, String value2) {
            addCriterion("face_url between", value1, value2, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andFaceUrlNotBetween(String value1, String value2) {
            addCriterion("face_url not between", value1, value2, "faceUrl");
            return (Criteria) this;
        }

        public Criteria andOrderStatusIsNull() {
            addCriterion("order_status is null");
            return (Criteria) this;
        }

        public Criteria andOrderStatusIsNotNull() {
            addCriterion("order_status is not null");
            return (Criteria) this;
        }

        public Criteria andOrderStatusEqualTo(String value) {
            addCriterion("order_status =", value, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusNotEqualTo(String value) {
            addCriterion("order_status <>", value, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusGreaterThan(String value) {
            addCriterion("order_status >", value, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusGreaterThanOrEqualTo(String value) {
            addCriterion("order_status >=", value, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusLessThan(String value) {
            addCriterion("order_status <", value, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusLessThanOrEqualTo(String value) {
            addCriterion("order_status <=", value, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusLike(String value) {
            addCriterion("order_status like", value, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusNotLike(String value) {
            addCriterion("order_status not like", value, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusIn(List<String> values) {
            addCriterion("order_status in", values, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusNotIn(List<String> values) {
            addCriterion("order_status not in", values, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusBetween(String value1, String value2) {
            addCriterion("order_status between", value1, value2, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andOrderStatusNotBetween(String value1, String value2) {
            addCriterion("order_status not between", value1, value2, "orderStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusIsNull() {
            addCriterion("putout_status is null");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusIsNotNull() {
            addCriterion("putout_status is not null");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusEqualTo(String value) {
            addCriterion("putout_status =", value, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusNotEqualTo(String value) {
            addCriterion("putout_status <>", value, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusGreaterThan(String value) {
            addCriterion("putout_status >", value, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusGreaterThanOrEqualTo(String value) {
            addCriterion("putout_status >=", value, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusLessThan(String value) {
            addCriterion("putout_status <", value, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusLessThanOrEqualTo(String value) {
            addCriterion("putout_status <=", value, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusLike(String value) {
            addCriterion("putout_status like", value, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusNotLike(String value) {
            addCriterion("putout_status not like", value, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusIn(List<String> values) {
            addCriterion("putout_status in", values, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusNotIn(List<String> values) {
            addCriterion("putout_status not in", values, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusBetween(String value1, String value2) {
            addCriterion("putout_status between", value1, value2, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andPutoutStatusNotBetween(String value1, String value2) {
            addCriterion("putout_status not between", value1, value2, "putoutStatus");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthIsNull() {
            addCriterion("is_push_earth is null");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthIsNotNull() {
            addCriterion("is_push_earth is not null");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthEqualTo(String value) {
            addCriterion("is_push_earth =", value, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthNotEqualTo(String value) {
            addCriterion("is_push_earth <>", value, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthGreaterThan(String value) {
            addCriterion("is_push_earth >", value, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthGreaterThanOrEqualTo(String value) {
            addCriterion("is_push_earth >=", value, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthLessThan(String value) {
            addCriterion("is_push_earth <", value, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthLessThanOrEqualTo(String value) {
            addCriterion("is_push_earth <=", value, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthLike(String value) {
            addCriterion("is_push_earth like", value, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthNotLike(String value) {
            addCriterion("is_push_earth not like", value, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthIn(List<String> values) {
            addCriterion("is_push_earth in", values, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthNotIn(List<String> values) {
            addCriterion("is_push_earth not in", values, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthBetween(String value1, String value2) {
            addCriterion("is_push_earth between", value1, value2, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andIsPushEarthNotBetween(String value1, String value2) {
            addCriterion("is_push_earth not between", value1, value2, "isPushEarth");
            return (Criteria) this;
        }

        public Criteria andTemplateIsNull() {
            addCriterion("template is null");
            return (Criteria) this;
        }

        public Criteria andTemplateIsNotNull() {
            addCriterion("template is not null");
            return (Criteria) this;
        }

        public Criteria andTemplateEqualTo(String value) {
            addCriterion("template =", value, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateNotEqualTo(String value) {
            addCriterion("template <>", value, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateGreaterThan(String value) {
            addCriterion("template >", value, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateGreaterThanOrEqualTo(String value) {
            addCriterion("template >=", value, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateLessThan(String value) {
            addCriterion("template <", value, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateLessThanOrEqualTo(String value) {
            addCriterion("template <=", value, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateLike(String value) {
            addCriterion("template like", value, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateNotLike(String value) {
            addCriterion("template not like", value, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateIn(List<String> values) {
            addCriterion("template in", values, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateNotIn(List<String> values) {
            addCriterion("template not in", values, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateBetween(String value1, String value2) {
            addCriterion("template between", value1, value2, "template");
            return (Criteria) this;
        }

        public Criteria andTemplateNotBetween(String value1, String value2) {
            addCriterion("template not between", value1, value2, "template");
            return (Criteria) this;
        }

        public Criteria andCrifScoreIsNull() {
            addCriterion("crif_score is null");
            return (Criteria) this;
        }

        public Criteria andCrifScoreIsNotNull() {
            addCriterion("crif_score is not null");
            return (Criteria) this;
        }

        public Criteria andCrifScoreEqualTo(String value) {
            addCriterion("crif_score =", value, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreNotEqualTo(String value) {
            addCriterion("crif_score <>", value, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreGreaterThan(String value) {
            addCriterion("crif_score >", value, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreGreaterThanOrEqualTo(String value) {
            addCriterion("crif_score >=", value, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreLessThan(String value) {
            addCriterion("crif_score <", value, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreLessThanOrEqualTo(String value) {
            addCriterion("crif_score <=", value, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreLike(String value) {
            addCriterion("crif_score like", value, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreNotLike(String value) {
            addCriterion("crif_score not like", value, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreIn(List<String> values) {
            addCriterion("crif_score in", values, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreNotIn(List<String> values) {
            addCriterion("crif_score not in", values, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreBetween(String value1, String value2) {
            addCriterion("crif_score between", value1, value2, "crifScore");
            return (Criteria) this;
        }

        public Criteria andCrifScoreNotBetween(String value1, String value2) {
            addCriterion("crif_score not between", value1, value2, "crifScore");
            return (Criteria) this;
        }

        public Criteria andPassRateIsNull() {
            addCriterion("pass_rate is null");
            return (Criteria) this;
        }

        public Criteria andPassRateIsNotNull() {
            addCriterion("pass_rate is not null");
            return (Criteria) this;
        }

        public Criteria andPassRateEqualTo(BigDecimal value) {
            addCriterion("pass_rate =", value, "passRate");
            return (Criteria) this;
        }

        public Criteria andPassRateNotEqualTo(BigDecimal value) {
            addCriterion("pass_rate <>", value, "passRate");
            return (Criteria) this;
        }

        public Criteria andPassRateGreaterThan(BigDecimal value) {
            addCriterion("pass_rate >", value, "passRate");
            return (Criteria) this;
        }

        public Criteria andPassRateGreaterThanOrEqualTo(BigDecimal value) {
            addCriterion("pass_rate >=", value, "passRate");
            return (Criteria) this;
        }

        public Criteria andPassRateLessThan(BigDecimal value) {
            addCriterion("pass_rate <", value, "passRate");
            return (Criteria) this;
        }

        public Criteria andPassRateLessThanOrEqualTo(BigDecimal value) {
            addCriterion("pass_rate <=", value, "passRate");
            return (Criteria) this;
        }

        public Criteria andPassRateIn(List<BigDecimal> values) {
            addCriterion("pass_rate in", values, "passRate");
            return (Criteria) this;
        }

        public Criteria andPassRateNotIn(List<BigDecimal> values) {
            addCriterion("pass_rate not in", values, "passRate");
            return (Criteria) this;
        }

        public Criteria andPassRateBetween(BigDecimal value1, BigDecimal value2) {
            addCriterion("pass_rate between", value1, value2, "passRate");
            return (Criteria) this;
        }

        public Criteria andPassRateNotBetween(BigDecimal value1, BigDecimal value2) {
            addCriterion("pass_rate not between", value1, value2, "passRate");
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

        public Criteria andIsRepeatLoanIsNull() {
            addCriterion("is_repeat_loan is null");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanIsNotNull() {
            addCriterion("is_repeat_loan is not null");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanEqualTo(String value) {
            addCriterion("is_repeat_loan =", value, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanNotEqualTo(String value) {
            addCriterion("is_repeat_loan <>", value, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanGreaterThan(String value) {
            addCriterion("is_repeat_loan >", value, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanGreaterThanOrEqualTo(String value) {
            addCriterion("is_repeat_loan >=", value, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanLessThan(String value) {
            addCriterion("is_repeat_loan <", value, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanLessThanOrEqualTo(String value) {
            addCriterion("is_repeat_loan <=", value, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanLike(String value) {
            addCriterion("is_repeat_loan like", value, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanNotLike(String value) {
            addCriterion("is_repeat_loan not like", value, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanIn(List<String> values) {
            addCriterion("is_repeat_loan in", values, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanNotIn(List<String> values) {
            addCriterion("is_repeat_loan not in", values, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanBetween(String value1, String value2) {
            addCriterion("is_repeat_loan between", value1, value2, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsRepeatLoanNotBetween(String value1, String value2) {
            addCriterion("is_repeat_loan not between", value1, value2, "isRepeatLoan");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlIsNull() {
            addCriterion("is_open_url is null");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlIsNotNull() {
            addCriterion("is_open_url is not null");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlEqualTo(String value) {
            addCriterion("is_open_url =", value, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlNotEqualTo(String value) {
            addCriterion("is_open_url <>", value, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlGreaterThan(String value) {
            addCriterion("is_open_url >", value, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlGreaterThanOrEqualTo(String value) {
            addCriterion("is_open_url >=", value, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlLessThan(String value) {
            addCriterion("is_open_url <", value, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlLessThanOrEqualTo(String value) {
            addCriterion("is_open_url <=", value, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlLike(String value) {
            addCriterion("is_open_url like", value, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlNotLike(String value) {
            addCriterion("is_open_url not like", value, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlIn(List<String> values) {
            addCriterion("is_open_url in", values, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlNotIn(List<String> values) {
            addCriterion("is_open_url not in", values, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlBetween(String value1, String value2) {
            addCriterion("is_open_url between", value1, value2, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andIsOpenUrlNotBetween(String value1, String value2) {
            addCriterion("is_open_url not between", value1, value2, "isOpenUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlIsNull() {
            addCriterion("face_video_url is null");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlIsNotNull() {
            addCriterion("face_video_url is not null");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlEqualTo(String value) {
            addCriterion("face_video_url =", value, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlNotEqualTo(String value) {
            addCriterion("face_video_url <>", value, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlGreaterThan(String value) {
            addCriterion("face_video_url >", value, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlGreaterThanOrEqualTo(String value) {
            addCriterion("face_video_url >=", value, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlLessThan(String value) {
            addCriterion("face_video_url <", value, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlLessThanOrEqualTo(String value) {
            addCriterion("face_video_url <=", value, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlLike(String value) {
            addCriterion("face_video_url like", value, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlNotLike(String value) {
            addCriterion("face_video_url not like", value, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlIn(List<String> values) {
            addCriterion("face_video_url in", values, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlNotIn(List<String> values) {
            addCriterion("face_video_url not in", values, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlBetween(String value1, String value2) {
            addCriterion("face_video_url between", value1, value2, "faceVideoUrl");
            return (Criteria) this;
        }

        public Criteria andFaceVideoUrlNotBetween(String value1, String value2) {
            addCriterion("face_video_url not between", value1, value2, "faceVideoUrl");
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