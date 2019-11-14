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

import com.paytm.daas.springmongo.dao.StoresDao;
import com.paytm.daas.springmongo.dto.response.SuccessResponse;
import com.paytm.daas.springmongo.entities.Stores;
import com.paytm.daas.springmongo.enums.ErrorMessages;
import com.paytm.daas.springmongo.exceptions.InvalidMongoException;
import com.paytm.daas.springmongo.utils.LogMessageGenerator;

@Repository
public class DefaultStoresDao implements StoresDao {
    private MongoOperations mongoOperations;
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStoresDao.class);
    
    @Autowired
    public DefaultStoresDao(MongoOperations mongoOperations) {
        this.mongoOperations = mongoOperations;
    }

    @Override
    public Stores getStoreById(Long storeId) throws InvalidMongoException {
        Long start = System.currentTimeMillis();
        LOGGER.info("Fetching the data from the DB.");
        
        Query query = new Query();
        query.addCriteria(new Criteria().where("store_id").is(storeId));
        Stores store = mongoOperations.findOne(query, Stores.class);
        if(Objects.isNull(store))
            throw new InvalidMongoException(ErrorMessages.NO_DATA_FOUND_FOR_GIVEN_ID);          
        
        Long timeTaken = System.currentTimeMillis() - start;
        LOGGER.info(LogMessageGenerator.getLogMessage("Time taken to fetch the data fron DB.", timeTaken));
        
        return store;
    }

    @Override
    public List<Stores> getAll() {
        Long start = System.currentTimeMillis();
        LOGGER.info("Fetching data from the DB.");
        
        List<Stores> storeList = mongoOperations.findAll(Stores.class);
        
        Long timeTaken = System.currentTimeMillis() - start;
        LOGGER.info(LogMessageGenerator.getLogMessage("Time taken to fetch the data", timeTaken));
        
        return storeList;
    }

    @Override
    public SuccessResponse updateById(Long storeId, String storeName) throws InvalidMongoException {
        Long start = System.currentTimeMillis();
        LOGGER.info("Updating the data in the DB.");
        
        Query query = new Query();
        query.addCriteria(new Criteria().where("store_id").is(storeId));
        Update update = new Update();
        update.set("store_name", storeName);
        Stores store = mongoOperations.findAndModify(query, update, Stores.class);
        if (Objects.isNull(store))
            throw new InvalidMongoException(ErrorMessages.NO_DATA_FOUND_FOR_GIVEN_ID);
        
        Long timeTaken = System.currentTimeMillis() - start;
        LOGGER.info(LogMessageGenerator.getLogMessage("Time taken to Update the data from the DB", timeTaken));
        return new SuccessResponse("SUCCESS", "Successfully Updated the feedback.");
    }

}
