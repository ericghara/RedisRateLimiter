package com.ericgha.exception;

public class QueryOutOfRangeException extends Exception {
    public QueryOutOfRangeException() {
        super();
    }

    public QueryOutOfRangeException(String message) {
        super( message );
    }

    public QueryOutOfRangeException(String message, Throwable cause) {
        super( message, cause );
    }

    public QueryOutOfRangeException(Throwable cause) {
        super( cause );
    }

    protected QueryOutOfRangeException(String message, Throwable cause, boolean enableSuppression,
                                       boolean writableStackTrace) {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}
