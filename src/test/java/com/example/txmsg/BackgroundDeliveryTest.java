package com.example.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

/** The background poller picks up pending messages and marks them SENT. */
class BackgroundDeliveryTest {

    @Test
    void deliversPendingMessageInBackgroundAndMarksSent() {
        TransactionalDatabase db = new TransactionalDatabase();
        CopyOnWriteArrayList<String> deliveredPayloads = new CopyOnWriteArrayList<>();
        DeduplicatingConsumer consumer = new DeduplicatingConsumer(m -> deliveredPayloads.add(m.payload()));

        AtomicReference<String> messageId = new AtomicReference<>();
        db.inTransaction(tx -> {
            OutboxMessage m = tx.enqueue("order-7", "OrderCreated", "hello");
            messageId.set(m.id());
        });

        try (MessageRelay relay = new MessageRelay(db.messageStore(), consumer, Clock.system(), 20, 10, 5)) {
            relay.start();
            Await.until("message delivered", () -> !deliveredPayloads.isEmpty());
        }

        OutboxMessage stored = db.messageStore().find(messageId.get());
        assertThat(stored.status()).isEqualTo(MessageStatus.SENT);
        assertThat(stored.attempts()).isEqualTo(1);
        assertThat(deliveredPayloads).containsExactly("hello");
    }

    @Test
    void picksUpMessagesEnqueuedWhileRunning() {
        TransactionalDatabase db = new TransactionalDatabase();
        CopyOnWriteArrayList<String> deliveredPayloads = new CopyOnWriteArrayList<>();
        DeduplicatingConsumer consumer = new DeduplicatingConsumer(m -> deliveredPayloads.add(m.payload()));

        try (MessageRelay relay = new MessageRelay(db.messageStore(), consumer, Clock.system(), 20, 10, 5)) {
            relay.start();
            db.inTransaction(tx -> tx.enqueue("order-8", "OrderCreated", "late"));
            Await.until("late message delivered", () -> deliveredPayloads.contains("late"));
        }
        assertThat(deliveredPayloads).containsExactly("late");
    }
}
