package com.paytm.daas.springmongo.dto.response;

public class SuccessResponse extends APIResponse {
    private Object data;

    public SuccessResponse(String status, Object data) {
        super(status);
        this.data = data;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
