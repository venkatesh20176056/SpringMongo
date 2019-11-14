package com.paytm.daas.springmongo.dao.impl;

import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import com.paytm.daas.springmongo.dao.TransactionHistoryDao;
import com.paytm.daas.springmongo.dto.response.SuccessResponse;
import com.paytm.daas.springmongo.entities.TransactionHistory;
import com.paytm.daas.springmongo.enums.ErrorMessages;
import com.paytm.daas.springmongo.exceptions.InvalidMongoException;
import com.paytm.daas.springmongo.utils.LogMessageGenerator;

@Repository
public class DefaultTransactionHistoryDao implements TransactionHistoryDao{

    private MongoOperations mongoOperations;
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTransactionHistoryDao.class);

    @Autowired
    public DefaultTransactionHistoryDao(MongoOperations mongoOperations) {
        this.mongoOperations = mongoOperations;
    }

    @Override
    public List<TransactionHistory> getByTransactionType(String transactionType) throws InvalidMongoException {
        Long start = System.currentTimeMillis();
        LOGGER.info("Fetching data from the DB");

        Query query = new Query();
        query.addCriteria(new Criteria().where("transaction_type").is(transactionType));
        List<TransactionHistory> transactionHistoryList = mongoOperations.find(query, TransactionHistory.class);

        if(CollectionUtils.isEmpty(transactionHistoryList)) 
            throw new InvalidMongoException(ErrorMessages.NO_DATA_FOR_GIVEN_TXN_TYPE);

        Long timeTaken = System.currentTimeMillis() - start;
        LOGGER.info(LogMessageGenerator.getLogMessage("Time taken to fetch the data from the DB", timeTaken));

        return transactionHistoryList;
    }

    @Override
    public List<TransactionHistory> getAllTransactions() {
        Long start = System.currentTimeMillis();
        LOGGER.info("Fetching data from DB.");

        List<TransactionHistory> transactionHistoryList = mongoOperations.findAll(TransactionHistory.class);

        Long timeTaken = System.currentTimeMillis() - start;
        LOGGER.info(LogMessageGenerator.getLogMessage("Time taken to fetch the data from the DB.", timeTaken));

        return transactionHistoryList;
    }

    @Override
    public SuccessResponse update(String id, String bankName) throws InvalidMongoException {
        Long start = System.currentTimeMillis();
        LOGGER.info("Updating the Transaction details based on ID.");

        Query query = new Query();
        query.addCriteria(new Criteria().where("_id").is(id));
        Update update = new Update();
        update.set("transaction_bank", bankName);
        TransactionHistory transactionHistory = mongoOperations.findAndModify(query, update, TransactionHistory.class);

        if(Objects.isNull(transactionHistory))
            throw new InvalidMongoException(ErrorMessages.NO_DATA_FOR_GIVEN_ID);

        Long timeTaken = System.currentTimeMillis() - start;
        LOGGER.info(LogMessageGenerator.getLogMessage("Time taken to update the data", timeTaken));

        return new SuccessResponse("SUCCESS", "Successfully updated...");
    }

    @Override
    public List<TransactionHistory> filter(String bankName, Integer count) throws InvalidMongoException {
        Long start = System.currentTimeMillis();
        LOGGER.info("Fetching data from the DB.");

        Query query = new Query();
        query.addCriteria(new Criteria().where("transaction_bank").is(bankName));
        query.limit(count);
        List<TransactionHistory> transactionalHistoryList = mongoOperations.find(query, TransactionHistory.class);
        if (CollectionUtils.isEmpty(transactionalHistoryList))
            throw new InvalidMongoException(ErrorMessages.NO_DATA_FOUND_FOR_GIVEN_BANK);

        Long timeTaken = System.currentTimeMillis() - start;
        LOGGER.info(LogMessageGenerator.getLogMessage("Time taken to get the data from DB", timeTaken));

        return transactionalHistoryList;
    }

}
