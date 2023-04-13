package com.ericgha.exception;

public class DirtyStateException extends RetryableException {
    public DirtyStateException() {
        super();
    }

    public DirtyStateException(String message) {
        super( message );
    }

    public DirtyStateException(String message, Throwable cause) {
        super( message, cause );
    }

    public DirtyStateException(Throwable cause) {
        super( cause );
    }

    protected DirtyStateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}
