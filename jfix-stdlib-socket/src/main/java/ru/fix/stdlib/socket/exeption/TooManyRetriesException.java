package ru.fix.stdlib.socket.exeption;

public class TooManyRetriesException extends RuntimeException {

    public TooManyRetriesException() {
        super();
    }

    public TooManyRetriesException(String message) {
        super(message);
    }
}
