package com.paytm.daas.springmongo.enums;

import org.springframework.http.HttpStatus;

public enum ErrorMessages {

    NO_DATA_FOR_GIVEN_TXN_TYPE(1, HttpStatus.BAD_REQUEST, "NO Data exist for given Transaction type"),
    NO_DATA_FOR_GIVEN_ID(2, HttpStatus.BAD_REQUEST, "No Data for given ID so cannot update..."),
    NO_DATA_FOUND_FOR_GIVEN_ID(3, HttpStatus.BAD_REQUEST, "No Data exist for given ID."),
    NO_DATA_FOUND_FOR_GIVEN_BANK(4, HttpStatus.BAD_REQUEST, "NO Data exist for given bank name");
    
    int code;
    HttpStatus status;
    String message;

    ErrorMessages(int code, HttpStatus status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;     
    }

    public String getMessage() {
        return message;
    }
    public int getCode() {
        return code;
    }
    public HttpStatus getStatus() {
        return status;
    }
    public void setMessage(String message) {
        this.message = message;
    }
    public void setCode(int code) {
        this.code = code;
    }
    public void setStatus(HttpStatus status) {
        this.status = status;
    }
}
