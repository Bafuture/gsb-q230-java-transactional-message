package com.example.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/**
 * Duplicate deliveries (same message id) take effect at most once downstream.
 * Duplicates happen in at-least-once delivery, e.g. crash after apply but
 * before the outbox row is marked SENT.
 */
class IdempotentConsumptionTest {

    @Test
    void duplicateDeliveriesApplyEffectOnce() {
        TransactionalDatabase db = new TransactionalDatabase();
        AtomicInteger effectCount = new AtomicInteger();
        DeduplicatingConsumer consumer =
                new DeduplicatingConsumer(m -> effectCount.incrementAndGet());

        final OutboxMessage[] holder = new OutboxMessage[1];
        db.inTransaction(tx -> holder[0] = tx.enqueue("order-9", "OrderCreated", "{}"));
        OutboxMessage message = db.messageStore().find(holder[0].id());

        consumer.consume(message);
        consumer.consume(message);
        consumer.consume(message);

        assertThat(effectCount).hasValue(1);
        assertThat(consumer.appliedMessages()).hasSize(1);
        assertThat(consumer.receivedMessages()).hasSize(3);
    }

    @Test
    void crashAfterApplyButBeforeMarkSentIsRedeliveredAndDeduplicated() {
        FakeClock clock = new FakeClock(0);
        TransactionalDatabase db = new TransactionalDatabase(new BusinessStore(), new MessageStore(), clock);
        AtomicInteger effectCount = new AtomicInteger();
        DeduplicatingConsumer downstream =
                new DeduplicatingConsumer(m -> effectCount.incrementAndGet());

        // Simulated network: the effect is applied (downstream dedup records it),
        // but the response is lost, so the relay treats the delivery as failed.
        final boolean[] firstAttempt = {true};
        MessageConsumer flakyNetwork = m -> {
            if (firstAttempt[0]) {
                firstAttempt[0] = false;
                downstream.consume(m);
                throw new RuntimeException("connection lost after apply");
            }
            downstream.consume(m);
        };
        MessageRelay relay = new MessageRelay(db.messageStore(), flakyNetwork, clock, 0, 50, 5);

        final OutboxMessage[] holder = new OutboxMessage[1];
        db.inTransaction(tx -> holder[0] = tx.enqueue("order-10", "OrderCreated", "{}"));

        relay.runOnce();
        assertThat(db.messageStore().find(holder[0].id()).status())
                .isEqualTo(MessageStatus.PENDING);
        clock.advanceMillis(50);
        relay.runOnce();

        assertThat(db.messageStore().find(holder[0].id()).status())
                .isEqualTo(MessageStatus.SENT);
        assertThat(effectCount).hasValue(1);
        assertThat(downstream.receivedMessages()).hasSize(2);
        assertThat(downstream.appliedMessages()).hasSize(1);
    }
}
