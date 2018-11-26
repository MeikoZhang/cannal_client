package com.sixku.canal.mybatis.domain;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OrderFlowExample {
    protected String orderByClause;

    protected boolean distinct;

    protected List<Criteria> oredCriteria;

    private Integer limit;

    private Integer offset;

    public OrderFlowExample() {
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

        public Criteria andFlowIdIsNull() {
            addCriterion("flow_id is null");
            return (Criteria) this;
        }

        public Criteria andFlowIdIsNotNull() {
            addCriterion("flow_id is not null");
            return (Criteria) this;
        }

        public Criteria andFlowIdEqualTo(Long value) {
            addCriterion("flow_id =", value, "flowId");
            return (Criteria) this;
        }

        public Criteria andFlowIdNotEqualTo(Long value) {
            addCriterion("flow_id <>", value, "flowId");
            return (Criteria) this;
        }

        public Criteria andFlowIdGreaterThan(Long value) {
            addCriterion("flow_id >", value, "flowId");
            return (Criteria) this;
        }

        public Criteria andFlowIdGreaterThanOrEqualTo(Long value) {
            addCriterion("flow_id >=", value, "flowId");
            return (Criteria) this;
        }

        public Criteria andFlowIdLessThan(Long value) {
            addCriterion("flow_id <", value, "flowId");
            return (Criteria) this;
        }

        public Criteria andFlowIdLessThanOrEqualTo(Long value) {
            addCriterion("flow_id <=", value, "flowId");
            return (Criteria) this;
        }

        public Criteria andFlowIdIn(List<Long> values) {
            addCriterion("flow_id in", values, "flowId");
            return (Criteria) this;
        }

        public Criteria andFlowIdNotIn(List<Long> values) {
            addCriterion("flow_id not in", values, "flowId");
            return (Criteria) this;
        }

        public Criteria andFlowIdBetween(Long value1, Long value2) {
            addCriterion("flow_id between", value1, value2, "flowId");
            return (Criteria) this;
        }

        public Criteria andFlowIdNotBetween(Long value1, Long value2) {
            addCriterion("flow_id not between", value1, value2, "flowId");
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

        public Criteria andStartStatusIsNull() {
            addCriterion("start_status is null");
            return (Criteria) this;
        }

        public Criteria andStartStatusIsNotNull() {
            addCriterion("start_status is not null");
            return (Criteria) this;
        }

        public Criteria andStartStatusEqualTo(String value) {
            addCriterion("start_status =", value, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusNotEqualTo(String value) {
            addCriterion("start_status <>", value, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusGreaterThan(String value) {
            addCriterion("start_status >", value, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusGreaterThanOrEqualTo(String value) {
            addCriterion("start_status >=", value, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusLessThan(String value) {
            addCriterion("start_status <", value, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusLessThanOrEqualTo(String value) {
            addCriterion("start_status <=", value, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusLike(String value) {
            addCriterion("start_status like", value, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusNotLike(String value) {
            addCriterion("start_status not like", value, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusIn(List<String> values) {
            addCriterion("start_status in", values, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusNotIn(List<String> values) {
            addCriterion("start_status not in", values, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusBetween(String value1, String value2) {
            addCriterion("start_status between", value1, value2, "startStatus");
            return (Criteria) this;
        }

        public Criteria andStartStatusNotBetween(String value1, String value2) {
            addCriterion("start_status not between", value1, value2, "startStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusIsNull() {
            addCriterion("end_status is null");
            return (Criteria) this;
        }

        public Criteria andEndStatusIsNotNull() {
            addCriterion("end_status is not null");
            return (Criteria) this;
        }

        public Criteria andEndStatusEqualTo(String value) {
            addCriterion("end_status =", value, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusNotEqualTo(String value) {
            addCriterion("end_status <>", value, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusGreaterThan(String value) {
            addCriterion("end_status >", value, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusGreaterThanOrEqualTo(String value) {
            addCriterion("end_status >=", value, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusLessThan(String value) {
            addCriterion("end_status <", value, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusLessThanOrEqualTo(String value) {
            addCriterion("end_status <=", value, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusLike(String value) {
            addCriterion("end_status like", value, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusNotLike(String value) {
            addCriterion("end_status not like", value, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusIn(List<String> values) {
            addCriterion("end_status in", values, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusNotIn(List<String> values) {
            addCriterion("end_status not in", values, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusBetween(String value1, String value2) {
            addCriterion("end_status between", value1, value2, "endStatus");
            return (Criteria) this;
        }

        public Criteria andEndStatusNotBetween(String value1, String value2) {
            addCriterion("end_status not between", value1, value2, "endStatus");
            return (Criteria) this;
        }

        public Criteria andApproveResultIsNull() {
            addCriterion("approve_result is null");
            return (Criteria) this;
        }

        public Criteria andApproveResultIsNotNull() {
            addCriterion("approve_result is not null");
            return (Criteria) this;
        }

        public Criteria andApproveResultEqualTo(String value) {
            addCriterion("approve_result =", value, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultNotEqualTo(String value) {
            addCriterion("approve_result <>", value, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultGreaterThan(String value) {
            addCriterion("approve_result >", value, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultGreaterThanOrEqualTo(String value) {
            addCriterion("approve_result >=", value, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultLessThan(String value) {
            addCriterion("approve_result <", value, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultLessThanOrEqualTo(String value) {
            addCriterion("approve_result <=", value, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultLike(String value) {
            addCriterion("approve_result like", value, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultNotLike(String value) {
            addCriterion("approve_result not like", value, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultIn(List<String> values) {
            addCriterion("approve_result in", values, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultNotIn(List<String> values) {
            addCriterion("approve_result not in", values, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultBetween(String value1, String value2) {
            addCriterion("approve_result between", value1, value2, "approveResult");
            return (Criteria) this;
        }

        public Criteria andApproveResultNotBetween(String value1, String value2) {
            addCriterion("approve_result not between", value1, value2, "approveResult");
            return (Criteria) this;
        }

        public Criteria andResultExplainIsNull() {
            addCriterion("result_explain is null");
            return (Criteria) this;
        }

        public Criteria andResultExplainIsNotNull() {
            addCriterion("result_explain is not null");
            return (Criteria) this;
        }

        public Criteria andResultExplainEqualTo(String value) {
            addCriterion("result_explain =", value, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainNotEqualTo(String value) {
            addCriterion("result_explain <>", value, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainGreaterThan(String value) {
            addCriterion("result_explain >", value, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainGreaterThanOrEqualTo(String value) {
            addCriterion("result_explain >=", value, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainLessThan(String value) {
            addCriterion("result_explain <", value, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainLessThanOrEqualTo(String value) {
            addCriterion("result_explain <=", value, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainLike(String value) {
            addCriterion("result_explain like", value, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainNotLike(String value) {
            addCriterion("result_explain not like", value, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainIn(List<String> values) {
            addCriterion("result_explain in", values, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainNotIn(List<String> values) {
            addCriterion("result_explain not in", values, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainBetween(String value1, String value2) {
            addCriterion("result_explain between", value1, value2, "resultExplain");
            return (Criteria) this;
        }

        public Criteria andResultExplainNotBetween(String value1, String value2) {
            addCriterion("result_explain not between", value1, value2, "resultExplain");
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