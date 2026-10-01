package com.example.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/** Failed deliveries are retried with backoff, then dead-lettered beyond the cap. */
class RetryAndDeadLetterTest {

    private static final int MAX_ATTEMPTS = 3;
    private static final long BASE_BACKOFF_MS = 100;

    @Test
    void retriesWithBackoffThenSucceeds() {
        FakeClock clock = new FakeClock(1000);
        TransactionalDatabase db = new TransactionalDatabase(new BusinessStore(), new MessageStore(), clock);
        AtomicInteger calls = new AtomicInteger();
        MessageConsumer flaky = m -> {
            if (calls.incrementAndGet() < 3) {
                throw new RuntimeException("downstream unavailable");
            }
        };
        MessageRelay relay = new MessageRelay(db.messageStore(), flaky, clock, 0, BASE_BACKOFF_MS, MAX_ATTEMPTS);

        OutboxMessage message = commitOne(db, "order-1");

        // Attempt 1 fails -> retry scheduled at 1000 + 100
        relay.runOnce();
        assertPendingRetry(message, calls, 1, 1100);

        clock.advanceMillis(99);
        relay.runOnce();
        // Still within backoff: no new attempt.
        assertPendingRetry(message, calls, 1, 1100);

        clock.advanceMillis(1);
        // Attempt 2 fails -> retry scheduled at 1100 + 200 (doubling backoff)
        relay.runOnce();
        assertPendingRetry(message, calls, 2, 1300);

        clock.advanceMillis(200);
        // Attempt 3 succeeds.
        relay.runOnce();
        OutboxMessage reloaded = db.messageStore().find(message.id());
        assertThat(reloaded.status()).isEqualTo(MessageStatus.SENT);
        assertThat(reloaded.attempts()).isEqualTo(3);
        assertThat(calls).hasValue(3);
    }

    @Test
    void marksDeadAfterMaxAttemptsAndRecordsReason() {
        FakeClock clock = new FakeClock(5000);
        TransactionalDatabase db = new TransactionalDatabase(new BusinessStore(), new MessageStore(), clock);
        AtomicInteger calls = new AtomicInteger();
        MessageConsumer alwaysFails = m -> {
            calls.incrementAndGet();
            throw new IllegalStateException("permanent failure");
        };
        MessageRelay relay = new MessageRelay(db.messageStore(), alwaysFails, clock, 0, BASE_BACKOFF_MS, MAX_ATTEMPTS);

        OutboxMessage message = commitOne(db, "order-2");

        relay.runOnce();
        relay.runOnce();
        assertThat(message.attempts()).isEqualTo(1); // not due again yet

        clock.advanceMillis(BASE_BACKOFF_MS);
        relay.runOnce();
        assertThat(message.attempts()).isEqualTo(2);

        clock.advanceMillis(BASE_BACKOFF_MS * 2L);
        relay.runOnce();
        // Third failed attempt: retry budget exhausted.
        OutboxMessage reloaded = db.messageStore().find(message.id());
        assertThat(reloaded.status()).isEqualTo(MessageStatus.DEAD);
        assertThat(reloaded.attempts()).isEqualTo(MAX_ATTEMPTS);
        assertThat(reloaded.lastError()).contains("permanent failure");
        assertThat(calls).hasValue(MAX_ATTEMPTS);

        // Time passes; a dead message must never be picked up again.
        clock.advanceMillis(100_000);
        relay.runOnce();
        assertThat(calls).hasValue(MAX_ATTEMPTS);
        assertThat(reloaded.status()).isEqualTo(MessageStatus.DEAD);
    }

    private static void assertPendingRetry(OutboxMessage message, AtomicInteger calls,
                                           int expectedAttempts, long expectedNextAttemptAt) {
        assertThat(message.status()).isEqualTo(MessageStatus.PENDING);
        assertThat(message.attempts()).isEqualTo(expectedAttempts);
        assertThat(calls).hasValue(expectedAttempts);
        assertThat(message.nextAttemptAtMillis()).isEqualTo(expectedNextAttemptAt);
    }

    private static OutboxMessage commitOne(TransactionalDatabase db, String key) {
        final OutboxMessage[] holder = new OutboxMessage[1];
        db.inTransaction(tx -> {
            holder[0] = tx.enqueue(key, "OrderCreated", "{}");
            tx.putBusiness(key, "CREATED");
        });
        return holder[0];
    }
}
