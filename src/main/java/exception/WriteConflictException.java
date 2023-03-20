package exception;

public class WriteConflictException extends RetryableException {
    public WriteConflictException() {
        super();
    }

    public WriteConflictException(String message) {
        super( message );
    }

    public WriteConflictException(String message, Throwable cause) {
        super( message, cause );
    }

    public WriteConflictException(Throwable cause) {
        super( cause );
    }

    protected WriteConflictException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}
