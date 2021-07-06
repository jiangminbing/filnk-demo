package com.example.entity;

/**
 * @author jiangmb
 * @version 1.0.0
 * @date 2021-07-05 18:43
 */
public class Transaction {
    private Integer accountId;

    private Double amount;

    public Integer getAccountId() {
        return accountId;
    }

    public void setAccountId(Integer accountId) {
        this.accountId = accountId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }
}
