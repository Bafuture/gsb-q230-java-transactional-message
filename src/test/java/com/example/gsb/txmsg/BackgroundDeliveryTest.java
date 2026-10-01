package com.example.gsb.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Requirement 2: the background poller delivers pending messages and marks them SENT. */
class BackgroundDeliveryTest {

    @Test
    void deliversPendingMessagesInBackgroundAndMarksThemSent() throws Exception {
        TestEndpoint endpoint = new TestEndpoint();
        try (TransactionalMessageService service = new TransactionalMessageService(
                LocalDatabase.inMemory(), endpoint, RetryPolicy.exponential(3, 10), 10)) {
            service.start();

            service.inTransaction(ctx -> {
                ctx.putBusinessData("acc-1", "opened");
                ctx.send("acc-1", "welcome-email");
                ctx.send("acc-2", "hello");
            });

            Await.until("both messages sent", () -> service.statistics().sentCount() == 2);

            assertThat(endpoint.deliveries()).hasSize(2);
            assertThat(endpoint.deliveries())
                    .extracting(MessageEnvelope::getPayload)
                    .containsExactlyInAnyOrder("welcome-email", "hello");
            for (MessageEnvelope message : service.database().messages()) {
                assertThat(message.getStatus()).isEqualTo(MessageStatus.SENT);
                assertThat(message.getSentAtMillis()).isPositive();
                assertThat(message.getAttempts()).isEqualTo(1);
            }
            assertThat(service.statistics().pendingCount()).isZero();
            assertThat(service.database().businessData()).containsEntry("acc-1", "opened");
        }
    }

    @Test
    void doesNotDeliverAfterDispatcherIsStopped() throws Exception {
        TestEndpoint endpoint = new TestEndpoint();
        TransactionalMessageService service = new TransactionalMessageService(
                LocalDatabase.inMemory(), endpoint, RetryPolicy.exponential(3, 10), 10);
        service.start();
        service.close();

        service.inTransaction(ctx -> ctx.send("key-a", "late"));
        Thread.sleep(200);

        assertThat(endpoint.deliveries()).isEmpty();
        assertThat(service.statistics().pendingCount()).isEqualTo(1);
        assertThat(service.statistics().sentCount()).isZero();
    }
}
