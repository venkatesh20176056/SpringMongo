package com.paytm.daas.springmongo.controllers;

import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.paytm.daas.springmongo.constants.URLConstants;
import com.paytm.daas.springmongo.dto.response.ErrorDetails;
import com.paytm.daas.springmongo.dto.response.ErrorResponse;
import com.paytm.daas.springmongo.dto.response.SuccessResponse;
import com.paytm.daas.springmongo.entities.Stores;
import com.paytm.daas.springmongo.entities.TransactionHistory;
import com.paytm.daas.springmongo.exceptions.InvalidMongoException;
import com.paytm.daas.springmongo.services.StoreService;
import com.paytm.daas.springmongo.services.TransactionHistoryService;
import com.paytm.daas.springmongo.utils.LogMessageGenerator;

@RestController
public class RequestsController {

    private TransactionHistoryService transactionHistoryService;
    private StoreService storeService;
    private static final Logger LOGGER = LoggerFactory.getLogger(RequestsController.class);

    @Autowired
    public RequestsController(TransactionHistoryService transactionHistoryService, StoreService storeService) {
        this.transactionHistoryService = transactionHistoryService;
        this.storeService = storeService;
    }

    @RequestMapping(value = URLConstants.GET_BY_TXN_TYPE, method = RequestMethod.GET)
    public ResponseEntity<?> getByTransactionType(@RequestParam String transactionType) throws InvalidMongoException{
        try {
            Long start = System.currentTimeMillis();
            LOGGER.info("Request recieved to fetch the data from DB.");

            List<TransactionHistory> transactionHistoryList = transactionHistoryService.getByTransactionType(transactionType);
            SuccessResponse successResponse = new SuccessResponse("SUCCESS", transactionHistoryList);
            ResponseEntity<SuccessResponse> response = new ResponseEntity<SuccessResponse> (successResponse, HttpStatus.OK);

            Long timeTaken = System.currentTimeMillis() - start;
            LOGGER.info(LogMessageGenerator.getLogMessage("Total time taken to fetch the data", timeTaken));

            return response;
        }
        catch (InvalidMongoException e) {
            ErrorResponse errorResponse = new ErrorResponse("Fail", new ErrorDetails(e.getMessage()));
            ResponseEntity<ErrorResponse> response = new ResponseEntity<ErrorResponse> (errorResponse, e.getErrorMessages().getStatus());
            return response;
        }
    }

    @RequestMapping(value = URLConstants.GET_ALL_TXNS, method = RequestMethod.GET)
    public ResponseEntity<?> getAllTransactions() {
        Long start = System.currentTimeMillis();
        LOGGER.info("Request recieved to fetch the data the data from DB.");

        SuccessResponse successResponse = new SuccessResponse("SUCCESS", transactionHistoryService.getAllTransactions());
        ResponseEntity<SuccessResponse> response = new ResponseEntity<SuccessResponse> (successResponse, HttpStatus.OK);

        Long timeTaken = System.currentTimeMillis() - start;
        LOGGER.info(LogMessageGenerator.getLogMessage("Total Time taken", timeTaken));

        return response;
    }

    @RequestMapping(value = URLConstants.UPDATE_BY_ID, method = RequestMethod.PATCH)
    public ResponseEntity<?> updateById(@RequestParam String id, @RequestParam String bankName) 
            throws InvalidMongoException {
        try {
            Long start = System.currentTimeMillis();
            LOGGER.info("Request recieved to fetch the data the data from DB.");

            ResponseEntity<SuccessResponse> response = new ResponseEntity<SuccessResponse> (transactionHistoryService.update(id, bankName), HttpStatus.OK);

            Long timeTaken = System.currentTimeMillis() - start;
            LOGGER.info(LogMessageGenerator.getLogMessage("Total Time taken", timeTaken));

            return response;
        } catch (InvalidMongoException e) {
            ErrorResponse errorResponse = new ErrorResponse("Fail", new ErrorDetails(e.getErrorMessages().getMessage()));
            return new ResponseEntity<ErrorResponse> (errorResponse, e.getErrorMessages().getStatus());  
        }
    }

    @RequestMapping(value = URLConstants.GET_FILTERED_DATA, method = RequestMethod.GET)
    public ResponseEntity<?> getFilteredData(@RequestParam String bankName, @RequestParam Integer count) throws InvalidMongoException {
        try {
            Long start = System.currentTimeMillis();
            LOGGER.info("Request recieved to fetch the data from the DB");
            
            SuccessResponse successResponse = new SuccessResponse("SUCCESS", transactionHistoryService.getFilteredData(bankName, count));
            ResponseEntity<SuccessResponse> response = new ResponseEntity<SuccessResponse> (successResponse, HttpStatus.OK);
            
            Long timeTaken = System.currentTimeMillis() - start;
            LOGGER.info(LogMessageGenerator.getLogMessage("Total time taken", timeTaken));
            
            return response;

        } catch (InvalidMongoException e) {
            ErrorResponse errorResponse = new ErrorResponse("FAIL", new ErrorDetails(e.getMessage()));
            ResponseEntity<ErrorResponse> response = new ResponseEntity<ErrorResponse> (errorResponse, e.getErrorMessages().getStatus());
            
            return response;

        }
    }
    //*********************************************************************************************************************************************

    @RequestMapping(value = URLConstants.GET_STORE_BY_ID, method = RequestMethod.GET)
    public ResponseEntity<?> getStoreById(@RequestParam Long storeId) throws InvalidMongoException {
        try {
            Long start = System.currentTimeMillis();
            LOGGER.info("Request recieved to fetch the data from DB");

            SuccessResponse successResponse = new SuccessResponse("SUCCESS", storeService.getStoreById(storeId));
            ResponseEntity<SuccessResponse> response = new ResponseEntity<SuccessResponse> (successResponse, HttpStatus.OK);

            Long timeTaken = System.currentTimeMillis() - start;
            LOGGER.info("Total time taken to fetch the results from DB", timeTaken);

            return response;
        } catch (InvalidMongoException e) {
            ErrorResponse errorResponse = new ErrorResponse("FAIL", new ErrorDetails(e.getMessage()));
            ResponseEntity<ErrorResponse> response = new ResponseEntity<ErrorResponse> (errorResponse, e.getErrorMessages().getStatus());
            return response;
        }
    }

    //*********************************************************************************************************************************************

    @RequestMapping(value = URLConstants.GET_ALL_STORES, method = RequestMethod.GET)
    public ResponseEntity<?> getAllStores() {
        Long start = System.currentTimeMillis();
        LOGGER.info("Request recieved to fetch data from the DB");

        ResponseEntity<List<Stores>> response = new ResponseEntity<List<Stores>>(storeService.getAll(), HttpStatus.OK);

        Long timeTaken = System.currentTimeMillis() - start;
        LOGGER.info(LogMessageGenerator.getLogMessage("Time taken to fetch the data from DB", timeTaken));
        return response;
    }
    //*********************************************************************************************************************************************

    @RequestMapping(value = URLConstants.UPDATE_BY_STORE_ID, method = RequestMethod.GET)
    public ResponseEntity<?> updateById(@RequestParam Long storeId, @RequestParam String storeName) {
        try {
            Long start = System.currentTimeMillis();
            LOGGER.info("Request received to update the DB for given ID.");

            ResponseEntity<SuccessResponse> response = new ResponseEntity<SuccessResponse>(storeService.updateById(storeId, storeName), HttpStatus.OK);

            Long timeTaken = System.currentTimeMillis() - start;
            LOGGER.info(LogMessageGenerator.getLogMessage("Total time taken to Update.", timeTaken));

            return response;

        } catch(InvalidMongoException e) {
            ErrorResponse errorResponse = new ErrorResponse("FAIL", new ErrorDetails(e.getMessage()));
            ResponseEntity<ErrorResponse> response = new ResponseEntity<ErrorResponse>(errorResponse, e.getErrorMessages().getStatus());
            return response;
        }
    }

    //*********************************************************************************************************************************************
}
