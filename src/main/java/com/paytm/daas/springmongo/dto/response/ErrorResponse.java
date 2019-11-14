package com.paytm.daas.springmongo.dto.response;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ErrorResponse extends APIResponse {
    @JsonProperty(value = "error")
    private ErrorDetails errorDetails;

    public ErrorResponse(String status, ErrorDetails errorDetails) {
        super(status);
        this.errorDetails = errorDetails;
    }

    public ErrorDetails getErrorDetails() {
        return errorDetails;
    }

    public void setErrorDetails(ErrorDetails errorDetails) {
        this.errorDetails = errorDetails;
    }
}
