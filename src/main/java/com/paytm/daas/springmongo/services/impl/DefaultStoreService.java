package com.paytm.daas.springmongo.services.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.paytm.daas.springmongo.dao.StoresDao;
import com.paytm.daas.springmongo.dto.response.SuccessResponse;
import com.paytm.daas.springmongo.entities.Stores;
import com.paytm.daas.springmongo.exceptions.InvalidMongoException;
import com.paytm.daas.springmongo.services.StoreService;

@Service
public class DefaultStoreService implements StoreService {
    private StoresDao storesDao;
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStoreService.class);
    
    @Autowired
    public DefaultStoreService(StoresDao storesDao) {
        this.storesDao = storesDao;
    }

    @Override
    public Stores getStoreById(Long storeId) throws InvalidMongoException {
        return storesDao.getStoreById(storeId);
    }

    @Override
    public List<Stores> getAll() {
        LOGGER.info("Trying to fetch the data from DB.");
        return storesDao.getAll();
    }

    @Override
    public SuccessResponse updateById(Long storeId, String storeName) throws InvalidMongoException {
        return storesDao.updateById(storeId, storeName);
    }

}
