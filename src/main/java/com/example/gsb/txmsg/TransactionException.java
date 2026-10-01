package com.example.gsb.txmsg;

/** Raised when the transactional component cannot guarantee durability. */
public class TransactionException extends RuntimeException {
    public TransactionException(String message, Throwable cause) {
        super(message, cause);
    }
}
