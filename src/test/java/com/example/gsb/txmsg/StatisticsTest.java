package com.example.gsb.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Requirement 7: counts, retry totals and per-state duration statistics. */
class StatisticsTest {

    @Test
    void snapshotReflectsMixedMessageStates() throws Exception {
        LocalDatabase database = LocalDatabase.inMemory();
        TestEndpoint endpoint = new TestEndpoint();
        endpoint.failWhen(message -> {
            int attempt = endpoint.attemptsFor(message.getId());
            return message.getPayload().equals("flaky") && attempt == 1
                    || message.getPayload().equals("doomed");
        });

        RetryPolicy retryPolicy = new RetryPolicy(2, 20, 2.0);
        MessageDispatcher dispatcher = new MessageDispatcher(database, endpoint, retryPolicy, 5);
        MessageStatistics statistics = new MessageStatistics(database);

        database.inTransaction(ctx -> {
            ctx.send("k1", "smooth");
            ctx.send("k2", "flaky");
            ctx.send("k3", "doomed");
        });

        dispatcher.pollOnce(); // smooth: SENT; flaky: fail(1); doomed: fail(1)
        Thread.sleep(40);      // wait beyond the 20ms retry backoff
        dispatcher.pollOnce(); // flaky: SENT; doomed: fail(2) -> DEAD_LETTER

        database.inTransaction(ctx -> ctx.send("k4", "waiting")); // never polled

        StatisticsSnapshot snapshot = statistics.snapshot();
        assertThat(snapshot.totalCount()).isEqualTo(4);
        assertThat(snapshot.sentCount()).isEqualTo(2);
        assertThat(snapshot.pendingCount()).isEqualTo(1);
        assertThat(snapshot.deadLetterCount()).isEqualTo(1);
        assertThat(snapshot.totalDeliveryAttempts()).isEqualTo(5);
        assertThat(snapshot.totalRetries()).isEqualTo(2);

        assertThat(snapshot.sendLatency().count()).isEqualTo(2);
        assertThat(snapshot.sendLatency().minMillis()).isGreaterThanOrEqualTo(0);
        assertThat(snapshot.sendLatency().maxMillis())
                .isGreaterThanOrEqualTo(snapshot.sendLatency().minMillis());

        assertThat(snapshot.pendingAge().count()).isEqualTo(1);
        assertThat(snapshot.pendingAge().minMillis()).isGreaterThanOrEqualTo(0);

        assertThat(snapshot.deadLetterLatency().count()).isEqualTo(1);
        assertThat(snapshot.deadLetterLatency().minMillis()).isGreaterThanOrEqualTo(20);

        assertThat(snapshot.backoffWait().count()).isEqualTo(2);
        assertThat(snapshot.backoffWait().totalMillis()).isEqualTo(40);
    }

    @Test
    void emptyStoreReportsEmptyStatistics() {
        StatisticsSnapshot snapshot =
                new MessageStatistics(LocalDatabase.inMemory()).snapshot();

        assertThat(snapshot.totalCount()).isZero();
        assertThat(snapshot.pendingCount()).isZero();
        assertThat(snapshot.sentCount()).isZero();
        assertThat(snapshot.deadLetterCount()).isZero();
        assertThat(snapshot.totalDeliveryAttempts()).isZero();
        assertThat(snapshot.totalRetries()).isZero();
        assertThat(snapshot.sendLatency()).isEqualTo(DurationStats.EMPTY);
        assertThat(snapshot.pendingAge()).isEqualTo(DurationStats.EMPTY);
        assertThat(snapshot.deadLetterLatency()).isEqualTo(DurationStats.EMPTY);
        assertThat(snapshot.backoffWait()).isEqualTo(DurationStats.EMPTY);
    }
}
