package ru.fix.stdlib.batching;

public class MaxPendingOperationExceededException extends RuntimeException {
    private static final long serialVersionUID = -1116214220804859448L;

    public MaxPendingOperationExceededException(String message) {
        super(message);
    }

    public MaxPendingOperationExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}
