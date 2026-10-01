package com.example.gsb.txmsg;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background poller that delivers PENDING messages to the downstream
 * endpoint and persists the outcome after every attempt.
 *
 * <p>Ordering: for each business key only the oldest unfinished message is
 * eligible. A message that is backing off or dead-lettered blocks later
 * messages of the same key, so per-key production order is preserved.
 */
public final class MessageDispatcher implements AutoCloseable {

    private final LocalDatabase database;
    private final DownstreamEndpoint endpoint;
    private final RetryPolicy retryPolicy;
    private final long pollIntervalMillis;
    private ScheduledExecutorService executor;

    public MessageDispatcher(LocalDatabase database, DownstreamEndpoint endpoint,
                             RetryPolicy retryPolicy, long pollIntervalMillis) {
        this.database = Objects.requireNonNull(database, "database");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        if (pollIntervalMillis <= 0) {
            throw new IllegalArgumentException("pollIntervalMillis must be > 0");
        }
        this.pollIntervalMillis = pollIntervalMillis;
    }

    /** Starts the background polling loop (idempotent). */
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "transactional-message-dispatcher");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::pollSafely, 0, pollIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /** Runs one polling round synchronously. Useful for deterministic tests. */
    public void pollOnce() {
        long now = System.currentTimeMillis();
        for (MessageEnvelope message : deliverableHeads(now)) {
            deliver(message);
        }
    }

    /**
     * Oldest unfinished message per business key that is due for delivery.
     * Keys whose head is backing off or dead-lettered are skipped entirely.
     */
    private List<MessageEnvelope> deliverableHeads(long nowMillis) {
        Map<String, MessageEnvelope> headByKey = new LinkedHashMap<>();
        for (MessageEnvelope message : database.messages()) {
            if (message.getStatus() == MessageStatus.SENT) {
                continue;
            }
            headByKey.putIfAbsent(message.getBusinessKey(), message);
        }
        List<MessageEnvelope> deliverable = new ArrayList<>();
        for (MessageEnvelope head : headByKey.values()) {
            if (head.getStatus() == MessageStatus.PENDING
                    && head.getNextAttemptAtMillis() <= nowMillis) {
                deliverable.add(head);
            }
        }
        return deliverable;
    }

    private void deliver(MessageEnvelope message) {
        long startedAtMillis = System.currentTimeMillis();
        try {
            endpoint.deliver(message);
            database.markSent(message.getId(), startedAtMillis, System.currentTimeMillis());
        } catch (Exception failure) {
            database.recordFailure(message.getId(), retryPolicy,
                    startedAtMillis, System.currentTimeMillis(), failure.toString());
        }
    }

    private void pollSafely() {
        try {
            pollOnce();
        } catch (Throwable unexpected) {
            unexpected.printStackTrace();
        }
    }

    /** Stops the background loop and waits for an in-flight poll to finish. */
    @Override
    public synchronized void close() {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        executor = null;
    }
}
