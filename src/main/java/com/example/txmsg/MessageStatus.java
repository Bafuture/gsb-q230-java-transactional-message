package com.example.txmsg;

/** Lifecycle states of an outbox message. */
public enum MessageStatus {
    /** Written by business transaction, waiting to be delivered. */
    PENDING,
    /** Delivered to the downstream and acknowledged. */
    SENT,
    /** Retry budget exhausted; requires manual intervention. */
    DEAD
}
