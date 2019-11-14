package com.paytm.daas.springmongo.dao;

import java.util.List;

import com.paytm.daas.springmongo.dto.response.SuccessResponse;
import com.paytm.daas.springmongo.entities.TransactionHistory;
import com.paytm.daas.springmongo.exceptions.InvalidMongoException;

public interface TransactionHistoryDao {
    public List<TransactionHistory> getByTransactionType(String transactionType) throws InvalidMongoException;
    public List<TransactionHistory> getAllTransactions();
    public SuccessResponse update(String id, String bankName) throws InvalidMongoException;
    public List<TransactionHistory> filter(String bankName, Integer count) throws InvalidMongoException;
}
