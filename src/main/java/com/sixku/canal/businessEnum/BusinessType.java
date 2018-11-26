package com.sixku.canal.businessEnum;

public enum BusinessType {

    //注册
    REGISTER("udai_register"),
    //申请
    APPLY("udai_apply"),
    //进件
    LOAN("udai_loan"),
    //审批通过
    APPROVAL("udai_approval"),
    //签约
    CONTRACT("udai_contract"),
    //放款
    PUTOUT("udai_putout");

    private String name;

    BusinessType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
