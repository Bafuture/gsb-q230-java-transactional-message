package com.example.gsb.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Requirement 3: exponential backoff retry, then dead-lettering with a recorded reason. */
class RetryAndDeadLetterTest {

    @Test
    void retriesWithExponentialBackoffThenDeadLettersWithReason() throws Exception {
        TestEndpoint endpoint = new TestEndpoint();
        endpoint.failWhen(message -> true);

        try (TransactionalMessageService service = new TransactionalMessageService(
                LocalDatabase.inMemory(), endpoint, RetryPolicy.exponential(3, 60), 10)) {
            service.start();
            service.inTransaction(ctx -> ctx.send("payment-1", "charge"));

            Await.until("message dead-lettered", () -> service.statistics().deadLetterCount() == 1);

            MessageEnvelope message = service.database().messages().get(0);
            assertThat(message.getStatus()).isEqualTo(MessageStatus.DEAD_LETTER);
            assertThat(message.getAttempts()).isEqualTo(3);
            assertThat(message.getHistory()).hasSize(3);
            assertThat(message.getHistory())
                    .extracting(DeliveryAttempt::success)
                    .containsExactly(false, false, false);
            assertThat(message.getDeadLetterReason())
                    .contains("Delivery failed after 3 attempt(s)")
                    .contains("simulated downstream failure");
            assertThat(message.getTotalBackoffMillis()).isEqualTo(60 + 120);

            long firstBackoffGap = message.getHistory().get(1).startedAtMillis()
                    - message.getHistory().get(0).startedAtMillis();
            long secondBackoffGap = message.getHistory().get(2).startedAtMillis()
                    - message.getHistory().get(1).startedAtMillis();
            assertThat(firstBackoffGap).isGreaterThanOrEqualTo(50);
            assertThat(secondBackoffGap).isGreaterThanOrEqualTo(100);

            int callsAfterDeadLetter = endpoint.totalCalls();
            Thread.sleep(200);
            assertThat(endpoint.totalCalls()).isEqualTo(callsAfterDeadLetter);
            assertThat(service.statistics().deadLetterCount()).isEqualTo(1);
        }
    }

    @Test
    void succeedsAfterTransientFailuresAndRecordsAttemptHistory() throws Exception {
        TestEndpoint endpoint = new TestEndpoint();
        endpoint.failWhen(message -> endpoint.attemptsFor(message.getId()) <= 2);

        try (TransactionalMessageService service = new TransactionalMessageService(
                LocalDatabase.inMemory(), endpoint, RetryPolicy.exponential(5, 20), 10)) {
            service.start();
            service.inTransaction(ctx -> ctx.send("key-a", "flaky"));

            Await.until("message sent after retries", () -> service.statistics().sentCount() == 1);

            MessageEnvelope message = service.database().messages().get(0);
            assertThat(message.getStatus()).isEqualTo(MessageStatus.SENT);
            assertThat(message.getAttempts()).isEqualTo(3);
            assertThat(message.getHistory())
                    .extracting(DeliveryAttempt::success)
                    .containsExactly(false, false, true);

            StatisticsSnapshot stats = service.statistics();
            assertThat(stats.totalDeliveryAttempts()).isEqualTo(3);
            assertThat(stats.totalRetries()).isEqualTo(2);
            assertThat(stats.backoffWait().count()).isEqualTo(1);
            assertThat(stats.backoffWait().totalMillis()).isEqualTo(20 + 40);
        }
    }

    @Test
    void deadLetterBlocksSameKeyButDoesNotBlockOtherKeys() throws Exception {
        TestEndpoint endpoint = new TestEndpoint();
        endpoint.failWhen(message -> message.getPayload().equals("poison"));

        try (TransactionalMessageService service = new TransactionalMessageService(
                LocalDatabase.inMemory(), endpoint, RetryPolicy.exponential(2, 20), 10)) {
            service.start();
            service.inTransaction(ctx -> {
                ctx.send("key-a", "poison");
                ctx.send("key-a", "behind-poison");
                ctx.send("key-b", "unrelated");
            });

            Await.until("poison dead-lettered", () -> service.statistics().deadLetterCount() == 1);
            Await.until("other key still delivered",
                    () -> service.database().messages().stream()
                            .filter(m -> m.getPayload().equals("unrelated"))
                            .findFirst().orElseThrow()
                            .getStatus() == MessageStatus.SENT);
            Thread.sleep(150);

            MessageEnvelope blocked = service.database().messages().stream()
                    .filter(m -> m.getPayload().equals("behind-poison"))
                    .findFirst().orElseThrow();
            assertThat(blocked.getStatus()).isEqualTo(MessageStatus.PENDING);
            assertThat(blocked.getAttempts()).isZero();
        }
    }
}
