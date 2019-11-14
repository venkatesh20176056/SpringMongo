package com.paytm.daas.springmongo.dto.response;

public class APIResponse {
    private String status;

    public APIResponse(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
