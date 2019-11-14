package com.paytm.daas.springmongo.entities;

import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "stores")
public class Stores {
    
    @Field("store_id")
    private Integer storeId;
    
    @Field("store_name")
    private String storeName;
    
    @Field("monthly_transaction_amount")
    private Integer monthlyTransactionAmount;

    public Integer getStoreId() {
        return storeId;
    }

    public String getStoreName() {
        return storeName;
    }

    public Integer getMonthlyTransactionAmount() {
        return monthlyTransactionAmount;
    }

    public void setStoreId(Integer storeId) {
        this.storeId = storeId;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public void setMonthlyTransactionAmount(Integer monthlyTransactionAmount) {
        this.monthlyTransactionAmount = monthlyTransactionAmount;
    }
    
}
