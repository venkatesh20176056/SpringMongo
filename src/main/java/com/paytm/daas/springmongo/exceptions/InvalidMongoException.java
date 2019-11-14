package com.paytm.daas.springmongo.exceptions;

import com.paytm.daas.springmongo.enums.ErrorMessages;

public class InvalidMongoException extends Exception {
    
    private ErrorMessages errorMessages;
    
    public InvalidMongoException() {
        super();
    }
    
    public InvalidMongoException(String message) {
        super(message);
    }
    
    public InvalidMongoException(Throwable throwable) {
        super(throwable);
    }
    
    public InvalidMongoException(String message, Throwable throwable) {
        super(message, throwable);
    }
    
    public InvalidMongoException(ErrorMessages errorMessages) {
        super(errorMessages.getMessage());
        this.errorMessages = errorMessages;
    }
    
    public InvalidMongoException(ErrorMessages errorMessages, Throwable throwable) {
        super(errorMessages.getMessage(), throwable);
        this.errorMessages = errorMessages;
    }

    public ErrorMessages getErrorMessages() {
        return errorMessages;
    }
    
}
