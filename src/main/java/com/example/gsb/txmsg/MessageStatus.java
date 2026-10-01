package com.example.gsb.txmsg;

/**
 * Lifecycle state of a transactional message.
 */
public enum MessageStatus {
    /** Written in the local message table, waiting to be (re)delivered. */
    PENDING,
    /** Acknowledged by the downstream endpoint. */
    SENT,
    /** Retry budget exhausted; the message needs manual intervention. */
    DEAD_LETTER
}
