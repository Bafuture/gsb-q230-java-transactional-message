package com.example.gsb.txmsg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A message persisted in the local message table.
 *
 * <p>Instances are mutated only under the coordinating {@link LocalDatabase}
 * lock. Readers receive defensive copies via {@link #copy()}.
 */
public final class MessageEnvelope implements Serializable {
    private static final long serialVersionUID = 1L;

    /** {@code -1} means the timestamp has not been set yet. */
    public static final long NO_TIMESTAMP = -1L;

    private final String id;
    private final String businessKey;
    private final String payload;
    private final long sequence;
    private final long createdAtMillis;

    private MessageStatus status = MessageStatus.PENDING;
    private int attempts;
    private long nextAttemptAtMillis;
    private long firstAttemptAtMillis = NO_TIMESTAMP;
    private long sentAtMillis = NO_TIMESTAMP;
    private long deadLetteredAtMillis = NO_TIMESTAMP;
    private String deadLetterReason;
    private long totalBackoffMillis;
    private final List<DeliveryAttempt> history = new ArrayList<>();

    MessageEnvelope(String id, String businessKey, String payload, long sequence, long createdAtMillis) {
        this.id = id;
        this.businessKey = businessKey;
        this.payload = payload;
        this.sequence = sequence;
        this.createdAtMillis = createdAtMillis;
        this.nextAttemptAtMillis = createdAtMillis;
    }

    private MessageEnvelope(MessageEnvelope other) {
        this.id = other.id;
        this.businessKey = other.businessKey;
        this.payload = other.payload;
        this.sequence = other.sequence;
        this.createdAtMillis = other.createdAtMillis;
        this.status = other.status;
        this.attempts = other.attempts;
        this.nextAttemptAtMillis = other.nextAttemptAtMillis;
        this.firstAttemptAtMillis = other.firstAttemptAtMillis;
        this.sentAtMillis = other.sentAtMillis;
        this.deadLetteredAtMillis = other.deadLetteredAtMillis;
        this.deadLetterReason = other.deadLetterReason;
        this.totalBackoffMillis = other.totalBackoffMillis;
        this.history.addAll(other.history);
    }

    /** Records one delivery attempt, successful or not. */
    void recordAttempt(boolean success, long startedAtMillis, long finishedAtMillis, String error) {
        attempts++;
        if (firstAttemptAtMillis == NO_TIMESTAMP) {
            firstAttemptAtMillis = startedAtMillis;
        }
        history.add(new DeliveryAttempt(attempts, startedAtMillis, finishedAtMillis - startedAtMillis, success, error));
    }

    void markSent(long atMillis) {
        this.status = MessageStatus.SENT;
        this.sentAtMillis = atMillis;
    }

    void scheduleRetry(long nowMillis, long backoffMillis) {
        this.status = MessageStatus.PENDING;
        this.nextAttemptAtMillis = nowMillis + backoffMillis;
        this.totalBackoffMillis += backoffMillis;
    }

    void markDeadLetter(long atMillis, String reason) {
        this.status = MessageStatus.DEAD_LETTER;
        this.deadLetteredAtMillis = atMillis;
        this.deadLetterReason = reason;
    }

    /**
     * Re-enqueues the message for delivery. Used for manual recovery of
     * dead-lettered messages and to simulate post-crash redelivery.
     */
    void requeue(long nowMillis) {
        this.status = MessageStatus.PENDING;
        this.attempts = 0;
        this.nextAttemptAtMillis = nowMillis;
        this.sentAtMillis = NO_TIMESTAMP;
        this.deadLetteredAtMillis = NO_TIMESTAMP;
        this.deadLetterReason = null;
    }

    MessageEnvelope copy() {
        return new MessageEnvelope(this);
    }

    public String getId() {
        return id;
    }

    public String getBusinessKey() {
        return businessKey;
    }

    public String getPayload() {
        return payload;
    }

    public long getSequence() {
        return sequence;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public MessageStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public long getNextAttemptAtMillis() {
        return nextAttemptAtMillis;
    }

    public long getFirstAttemptAtMillis() {
        return firstAttemptAtMillis;
    }

    public long getSentAtMillis() {
        return sentAtMillis;
    }

    public long getDeadLetteredAtMillis() {
        return deadLetteredAtMillis;
    }

    public String getDeadLetterReason() {
        return deadLetterReason;
    }

    public long getTotalBackoffMillis() {
        return totalBackoffMillis;
    }

    public List<DeliveryAttempt> getHistory() {
        return List.copyOf(history);
    }
}
