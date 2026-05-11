package com.ds2api.pow;

/** Exception thrown when PoW solving fails. */
public class PowException extends RuntimeException {
    public PowException(String message) {
        super(message);
    }

    public PowException(String message, Throwable cause) {
        super(message, cause);
    }
}
