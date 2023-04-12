package com.ericgha.exception;

/**
 * To signal task interruption due to a shutdown signal.
 */
public class ForcedShutdownException extends RuntimeException {
    public ForcedShutdownException() {
        super();
    }

    public ForcedShutdownException(String message) {
        super( message );
    }

    public ForcedShutdownException(String message, Throwable cause) {
        super( message, cause );
    }

    public ForcedShutdownException(Throwable cause) {
        super( cause );
    }

    protected ForcedShutdownException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}
