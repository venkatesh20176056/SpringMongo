package com.paytm.daas.springmongo.entities;

import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "transaction_history")
public class TransactionHistory {
    
    @Field("transaction_bank")
    private String transactionBank;
    
    @Field("transaction_amount")
    private Long transactionAmount;
    
    @Field("transaction_time")
    private Long transactionTime;

    @Field("transaction_type")
    private String transactionType;
    

    public String getTransactionBank() {
        return transactionBank;
    }

    public void setTransactionBank(String transactionBank) {
        this.transactionBank = transactionBank;
    }

    public Long getTransactionAmount() {
        return transactionAmount;
    }

    public Long getTransactionTime() {
        return transactionTime;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionAmount(Long transactionAmount) {
        this.transactionAmount = transactionAmount;
    }

    public void setTransactionTime(Long transactionTime) {
        this.transactionTime = transactionTime;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }
    
}
