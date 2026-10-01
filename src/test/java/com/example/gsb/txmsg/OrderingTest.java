package com.example.gsb.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Requirement 6: messages of one business key are delivered in production order. */
class OrderingTest {

    @Test
    void preservesPerKeyOrderWhileInterleavingKeys() throws Exception {
        TestEndpoint endpoint = new TestEndpoint();
        try (TransactionalMessageService service = new TransactionalMessageService(
                LocalDatabase.inMemory(), endpoint, RetryPolicy.exponential(3, 10), 5)) {
            service.start();
            service.inTransaction(ctx -> {
                for (int i = 1; i <= 5; i++) {
                    ctx.send("key-a", "A" + i);
                    ctx.send("key-b", "B" + i);
                }
            });

            Await.until("all ten messages sent", () -> service.statistics().sentCount() == 10);

            assertThat(endpoint.deliveredPayloads("key-a"))
                    .containsExactly("A1", "A2", "A3", "A4", "A5");
            assertThat(endpoint.deliveredPayloads("key-b"))
                    .containsExactly("B1", "B2", "B3", "B4", "B5");
        }
    }

    @Test
    void laterMessagesWaitWhileHeadMessageIsRetried() throws Exception {
        TestEndpoint endpoint = new TestEndpoint();
        endpoint.failWhen(message ->
                message.getPayload().equals("A2") && endpoint.attemptsFor(message.getId()) == 1);

        try (TransactionalMessageService service = new TransactionalMessageService(
                LocalDatabase.inMemory(), endpoint, RetryPolicy.exponential(5, 30), 5)) {
            service.start();
            service.inTransaction(ctx -> {
                ctx.send("key-a", "A1");
                ctx.send("key-a", "A2");
                ctx.send("key-a", "A3");
            });

            Await.until("all three messages sent", () -> service.statistics().sentCount() == 3);

            assertThat(endpoint.deliveredPayloads("key-a")).containsExactly("A1", "A2", "A3");
            MessageEnvelope retried = service.database().messages().stream()
                    .filter(m -> m.getPayload().equals("A2"))
                    .findFirst().orElseThrow();
            assertThat(retried.getAttempts()).isEqualTo(2);
        }
    }

    @Test
    void sequencesAreMonotonicAcrossTransactionsAndKeys() {
        LocalDatabase database = LocalDatabase.inMemory();
        database.inTransaction(ctx -> {
            ctx.send("key-a", "a1");
            ctx.send("key-b", "b1");
        });
        database.inTransaction(ctx -> ctx.send("key-a", "a2"));

        assertThat(database.messages())
                .extracting(MessageEnvelope::getSequence)
                .containsExactly(1L, 2L, 3L);
    }
}
