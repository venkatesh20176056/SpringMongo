package com.paytm.daas.springmongo.services.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.paytm.daas.springmongo.dao.TransactionHistoryDao;
import com.paytm.daas.springmongo.dto.response.SuccessResponse;
import com.paytm.daas.springmongo.entities.TransactionHistory;
import com.paytm.daas.springmongo.exceptions.InvalidMongoException;
import com.paytm.daas.springmongo.services.TransactionHistoryService;

@Service
public class DefaultTransactionHistoryService implements TransactionHistoryService {
    
    private TransactionHistoryDao transactionHistoryDao;
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTransactionHistoryService.class);

    @Autowired
    public DefaultTransactionHistoryService(TransactionHistoryDao transactionHistoryDao) {
        this.transactionHistoryDao = transactionHistoryDao;
    }
    
    @Override
    public List<TransactionHistory> getByTransactionType(String transactionType) throws InvalidMongoException{
        return transactionHistoryDao.getByTransactionType(transactionType);
    }

    @Override
    public List<TransactionHistory> getAllTransactions() {
        return transactionHistoryDao.getAllTransactions();
    }

    @Override
    public SuccessResponse update(String id, String bankName) throws InvalidMongoException {
        return transactionHistoryDao.update(id, bankName);
    }

    @Override
    public List<TransactionHistory> getFilteredData(String bankName, Integer count) throws InvalidMongoException {
        return transactionHistoryDao.filter(bankName, count);
    }

}
