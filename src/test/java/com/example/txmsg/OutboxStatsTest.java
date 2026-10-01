package com.example.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/** Pending/sent/dead counts, attempts, retries, failures and state durations. */
class OutboxStatsTest {

    @Test
    void reportsCountsRetriesFailuresAndDurations() {
        FakeClock clock = new FakeClock(1000);
        TransactionalDatabase db = new TransactionalDatabase(new BusinessStore(), new MessageStore(), clock);
        AtomicInteger failingCalls = new AtomicInteger();
        // m-ok succeeds immediately; m-flaky fails twice then succeeds; m-dead always fails.
        MessageConsumer consumer = m -> {
            switch (m.payload()) {
                case "m-ok" -> { }
                case "m-flaky" -> {
                    if (failingCalls.incrementAndGet() <= 2) {
                        throw new RuntimeException("transient");
                    }
                }
                case "m-dead" -> throw new RuntimeException("permanent");
                default -> { }
            }
        };
        MessageRelay relay = new MessageRelay(db.messageStore(), consumer, clock, 0, 100, 3);

        db.inTransaction(tx -> {
            tx.enqueue("k", "t", "m-ok");
            tx.enqueue("k", "t", "m-flaky");
            tx.enqueue("k", "t", "m-dead");
        });

        relay.runOnce();            // m-ok sent; m-flaky attempt 1 fails (next at +100)
        clock.advanceMillis(100);
        relay.runOnce();            // m-flaky attempt 2 fails (next at +200)
        clock.advanceMillis(200);
        relay.runOnce();            // m-flaky attempt 3 succeeds; m-dead attempt 1 fails
        clock.advanceMillis(100);
        relay.runOnce();            // m-dead attempt 2 fails
        clock.advanceMillis(200);
        relay.runOnce();            // m-dead attempt 3 fails -> DEAD

        OutboxStats stats = OutboxStats.snapshot(db.messageStore(), clock.nowMillis());

        assertThat(stats.pendingCount()).isZero();
        assertThat(stats.sentCount()).isEqualTo(2);
        assertThat(stats.deadCount()).isEqualTo(1);

        // attempts: 1 (ok) + 3 (flaky) + 3 (dead) = 7
        assertThat(stats.totalAttempts()).isEqualTo(7);
        // retries = attempts after first: 0 + 2 + 2 = 4
        assertThat(stats.totalRetries()).isEqualTo(4);
        // failures = every failed attempt: 0 + 2 + 3 = 5
        assertThat(stats.totalFailures()).isEqualTo(5);

        // Latencies are derived from create/update timestamps.
        assertThat(stats.sentLatency().count()).isEqualTo(2);
        assertThat(stats.sentLatency().maxMillis()).isGreaterThanOrEqualTo(300);
        assertThat(stats.deadLatency().count()).isEqualTo(1);
        assertThat(stats.pendingAge()).isEqualTo(TimingStats.EMPTY);

        // Dead message carries the reason for manual handling.
        OutboxMessage dead = db.messageStore().snapshot().stream()
                .filter(m -> m.status() == MessageStatus.DEAD)
                .findFirst().orElseThrow();
        assertThat(dead.lastError()).contains("permanent");
    }

    @Test
    void pendingAgeGrowsWhileWaiting() {
        FakeClock clock = new FakeClock(0);
        TransactionalDatabase db = new TransactionalDatabase(new BusinessStore(), new MessageStore(), clock);
        db.inTransaction(tx -> tx.enqueue("k", "t", "m"));

        clock.advanceMillis(250);
        OutboxStats stats = OutboxStats.snapshot(db.messageStore(), clock.nowMillis());
        assertThat(stats.pendingCount()).isEqualTo(1);
        assertThat(stats.pendingAge().maxMillis()).isEqualTo(250);
    }
}
