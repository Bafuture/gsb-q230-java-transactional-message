package com.example.txmsg;

import java.util.Objects;

/**
 * A row of the local message table. The same instance is shared between the
 * {@link MessageStore} and the relay; all mutation happens under the store's
 * lock inside {@link MessageStore}.
 */
public final class OutboxMessage {

    private final String id;
    private final String businessKey;
    private final String type;
    private final String payload;

    private long sequence;
    private MessageStatus status;
    private int attempts;
    private long createdAtMillis;
    private long updatedAtMillis;
    private long nextAttemptAtMillis;
    private String lastError;

    public OutboxMessage(String id, String businessKey, String type, String payload) {
        this.id = Objects.requireNonNull(id, "id");
        this.businessKey = Objects.requireNonNull(businessKey, "businessKey");
        this.type = Objects.requireNonNull(type, "type");
        this.payload = payload == null ? "" : payload;
        this.status = MessageStatus.PENDING;
    }

    public String id() {
        return id;
    }

    /** Ordering key: messages sharing the same business key are delivered in production order. */
    public String businessKey() {
        return businessKey;
    }

    public String type() {
        return type;
    }

    public String payload() {
        return payload;
    }

    public long sequence() {
        return sequence;
    }

    public MessageStatus status() {
        return status;
    }

    public int attempts() {
        return attempts;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    public long updatedAtMillis() {
        return updatedAtMillis;
    }

    public long nextAttemptAtMillis() {
        return nextAttemptAtMillis;
    }

    public String lastError() {
        return lastError;
    }

    void assignCommitMetadata(long sequence, long nowMillis) {
        this.sequence = sequence;
        this.createdAtMillis = nowMillis;
        this.updatedAtMillis = nowMillis;
        this.nextAttemptAtMillis = nowMillis;
    }

    /** Registers one delivery attempt (attempts becomes 1 on the first try). */
    void recordAttempt(long nowMillis) {
        attempts++;
        updatedAtMillis = nowMillis;
    }

    void markSent(long nowMillis) {
        this.status = MessageStatus.SENT;
        this.updatedAtMillis = nowMillis;
        this.nextAttemptAtMillis = Long.MAX_VALUE;
    }

    void scheduleRetry(long nowMillis, long nextAttemptAtMillis, String error) {
        this.status = MessageStatus.PENDING;
        this.updatedAtMillis = nowMillis;
        this.nextAttemptAtMillis = nextAttemptAtMillis;
        this.lastError = error;
    }

    void markDead(long nowMillis, String error) {
        this.status = MessageStatus.DEAD;
        this.updatedAtMillis = nowMillis;
        this.nextAttemptAtMillis = Long.MAX_VALUE;
        this.lastError = error;
    }
}
