package com.ericgha.exception;

/**
 * Marker class for exceptions that may be retried.
 */
public abstract class RetryableException extends RuntimeException {
    public RetryableException() {
        super();
    }

    public RetryableException(String message) {
        super( message );
    }

    public RetryableException(String message, Throwable cause) {
        super( message, cause );
    }

    public RetryableException(Throwable cause) {
        super( cause );
    }

    protected RetryableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}