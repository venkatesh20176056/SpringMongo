package com.paytm.daas.springmongo.dao;

import java.util.List;

import com.paytm.daas.springmongo.dto.response.SuccessResponse;
import com.paytm.daas.springmongo.entities.Stores;
import com.paytm.daas.springmongo.exceptions.InvalidMongoException;

public interface StoresDao {
    public Stores getStoreById(Long storeId) throws InvalidMongoException;
    public SuccessResponse updateById(Long storeId, String storeName) throws InvalidMongoException;
    public List<Stores> getAll();
}
