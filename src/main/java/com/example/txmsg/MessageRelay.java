package com.example.txmsg;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background poller that delivers pending outbox messages to the downstream
 * consumer. A single relay thread delivers one message at a time, which keeps
 * per-business-key ordering intact.
 *
 * <p>Delivery is at-least-once: a crash between "downstream applied" and
 * "marked sent" causes a redelivery after restart, so the downstream must be
 * idempotent (see {@link DeduplicatingConsumer}).
 */
public final class MessageRelay implements AutoCloseable {

    private final MessageStore store;
    private final MessageConsumer consumer;
    private final Clock clock;
    private final long pollIntervalMillis;
    private final long baseBackoffMillis;
    private final int maxAttempts;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread thread;

    public MessageRelay(MessageStore store, MessageConsumer consumer, Clock clock,
                        long pollIntervalMillis, long baseBackoffMillis, int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.store = Objects.requireNonNull(store);
        this.consumer = Objects.requireNonNull(consumer);
        this.clock = Objects.requireNonNull(clock);
        this.pollIntervalMillis = pollIntervalMillis;
        this.baseBackoffMillis = baseBackoffMillis;
        this.maxAttempts = maxAttempts;
    }

    /** Starts the background polling thread (idempotent). */
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            thread = new Thread(this::runLoop, "outbox-relay");
            thread.setDaemon(true);
            thread.start();
        }
    }

    private void runLoop() {
        while (running.get()) {
            try {
                runOnce();
            } catch (RuntimeException ignored) {
                // A buggy consumer must not kill the relay loop.
            }
            try {
                Thread.sleep(pollIntervalMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Delivers every message that is due right now, re-evaluating after each
     * batch so that messages unblocked by a successful delivery in this pass
     * are delivered too. Exposed for deterministic tests; the background loop
     * calls the same method.
     */
    public void runOnce() {
        while (true) {
            List<OutboxMessage> due = store.dueMessages(clock.nowMillis());
            if (due.isEmpty()) {
                return;
            }
            for (OutboxMessage message : due) {
                deliver(message);
            }
        }
    }

    private void deliver(OutboxMessage message) {
        long now = clock.nowMillis();
        synchronized (store) {
            message.recordAttempt(now);
        }
        try {
            consumer.consume(message);
            store.markSent(message, clock.nowMillis());
        } catch (Exception e) {
            int attempts = message.attempts();
            if (attempts >= maxAttempts) {
                store.markDead(message, clock.nowMillis(), describe(e));
            } else {
                long delay = baseBackoffMillis << (attempts - 1);
                store.scheduleRetry(message, clock.nowMillis(), now + delay, describe(e));
            }
        }
    }

    private static String describe(Exception e) {
        String msg = e.getMessage();
        return e.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    /** Stops the background thread and waits for it to finish. */
    @Override
    public synchronized void close() {
        running.set(false);
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }
}
