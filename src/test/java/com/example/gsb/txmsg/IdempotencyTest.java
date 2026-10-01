package com.example.gsb.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Requirement 4: the downstream effect happens at most once per message ID. */
class IdempotencyTest {

    @Test
    void repeatedDeliveryOfSameMessageAppliesEffectOnce() throws Exception {
        LocalDatabase database = LocalDatabase.inMemory();
        database.inTransaction(ctx -> ctx.send("key-a", "payload"));
        MessageEnvelope message = database.messages().get(0);

        TestEndpoint delegate = new TestEndpoint();
        IdempotentConsumer consumer = IdempotentConsumer.inMemory(delegate);

        consumer.deliver(message);
        consumer.deliver(message);
        consumer.deliver(message);

        assertThat(delegate.deliveries()).hasSize(1);
        assertThat(delegate.deliveries().get(0).getId()).isEqualTo(message.getId());
        assertThat(consumer.appliedCount()).isEqualTo(1);
        assertThat(consumer.hasApplied(message.getId())).isTrue();
    }

    @Test
    void differentMessagesAreEachApplied() throws Exception {
        LocalDatabase database = LocalDatabase.inMemory();
        database.inTransaction(ctx -> {
            ctx.send("key-a", "one");
            ctx.send("key-a", "two");
        });

        TestEndpoint delegate = new TestEndpoint();
        IdempotentConsumer consumer = IdempotentConsumer.inMemory(delegate);

        for (MessageEnvelope message : database.messages()) {
            consumer.deliver(message);
        }

        assertThat(delegate.deliveries()).hasSize(2);
        assertThat(consumer.appliedCount()).isEqualTo(2);
    }

    @Test
    void dedupStateSurvivesRestart(@TempDir Path directory) throws Exception {
        LocalDatabase database = LocalDatabase.inMemory();
        database.inTransaction(ctx -> ctx.send("key-a", "payload"));
        MessageEnvelope message = database.messages().get(0);

        Path stateFile = directory.resolve("consumer-state.bin");
        TestEndpoint delegate = new TestEndpoint();

        IdempotentConsumer firstProcess = IdempotentConsumer.fileBacked(stateFile, delegate);
        firstProcess.deliver(message);

        IdempotentConsumer secondProcess = IdempotentConsumer.fileBacked(stateFile, delegate);
        secondProcess.deliver(message);

        assertThat(delegate.deliveries()).hasSize(1);
        assertThat(secondProcess.hasApplied(message.getId())).isTrue();
    }

    @Test
    void redeliveryAfterCrashIsAcknowledgedWithoutDoubleEffect() throws Exception {
        TestEndpoint delegate = new TestEndpoint();
        IdempotentConsumer consumer = IdempotentConsumer.inMemory(delegate);

        try (TransactionalMessageService service = new TransactionalMessageService(
                LocalDatabase.inMemory(), consumer, RetryPolicy.exponential(3, 10), 10)) {
            service.start();
            service.inTransaction(ctx -> ctx.send("key-a", "exactly-once-effect"));

            Await.until("initial delivery sent", () -> service.statistics().sentCount() == 1);
            String messageId = service.database().messages().get(0).getId();

            // Simulate a crash between applying the effect and persisting SENT:
            // the message becomes PENDING again and gets redelivered.
            service.requeue(messageId);
            Await.until("redelivery happened",
                    () -> service.database().messages().get(0).getHistory().size() == 2);

            assertThat(service.database().messages().get(0).getStatus()).isEqualTo(MessageStatus.SENT);
            assertThat(delegate.deliveries()).hasSize(1);
            assertThat(consumer.appliedCount()).isEqualTo(1);
        }
    }
}
