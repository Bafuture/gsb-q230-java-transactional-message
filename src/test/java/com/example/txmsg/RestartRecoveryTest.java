package com.example.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

/**
 * The message table is durable storage. Replacing the relay process simulates
 * a restart: unfinished messages continue delivery, completed ones are never
 * delivered again.
 */
class RestartRecoveryTest {

    @Test
    void resumesPendingMessagesAndSkipsSentOnesAfterRestart() {
        // The stores are the durable medium; both relay "processes" share them.
        BusinessStore durableBusiness = new BusinessStore();
        MessageStore durableMessages = new MessageStore();
        Clock clock = Clock.system();
        TransactionalDatabase db = new TransactionalDatabase(durableBusiness, durableMessages, clock);

        CopyOnWriteArrayList<String> effects = new CopyOnWriteArrayList<>();
        DeduplicatingConsumer consumer = new DeduplicatingConsumer(m -> effects.add(m.id()));

        String beforeRestartId = enqueue(db, "order-a");

        // First process delivers one message, then dies.
        MessageRelay relay1 = new MessageRelay(durableMessages, consumer, clock, 0, 10, 5);
        relay1.runOnce();
        assertThat(durableMessages.find(beforeRestartId).status()).isEqualTo(MessageStatus.SENT);
        relay1.close();

        // A new message is produced after the restart.
        String afterRestartId = enqueue(db, "order-b");

        // Restart with a brand new relay.
        MessageRelay relay2 = new MessageRelay(durableMessages, consumer, clock, 0, 10, 5);
        relay2.runOnce();
        relay2.close();

        // The old SENT message is not redelivered; the pending one is delivered.
        assertThat(durableMessages.find(beforeRestartId).status()).isEqualTo(MessageStatus.SENT);
        assertThat(durableMessages.find(afterRestartId).status()).isEqualTo(MessageStatus.SENT);
        assertThat(effects).containsExactly(beforeRestartId, afterRestartId);
    }

    @Test
    void restartMidRetryContinuesFromPersistedState() {
        FakeClock clock = new FakeClock(0);
        BusinessStore durableBusiness = new BusinessStore();
        MessageStore durableMessages = new MessageStore();
        TransactionalDatabase db = new TransactionalDatabase(durableBusiness, durableMessages, clock);

        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        MessageConsumer alwaysFails = m -> {
            calls.incrementAndGet();
            throw new RuntimeException("nope");
        };

        enqueue(db, "order-c");

        MessageRelay relay1 = new MessageRelay(durableMessages, alwaysFails, clock, 0, 100, 3);
        relay1.runOnce(); // attempt 1, backed off
        relay1.close();

        OutboxMessage persisted = durableMessages.snapshot().get(0);
        assertThat(persisted.attempts()).isEqualTo(1);
        assertThat(persisted.status()).isEqualTo(MessageStatus.PENDING);

        // New process: it does not retry early, and does not reset the attempt budget.
        MessageRelay relay2 = new MessageRelay(durableMessages, alwaysFails, clock, 0, 100, 3);
        relay2.runOnce();
        assertThat(calls).hasValue(1);
        clock.advanceMillis(100);
        relay2.runOnce(); // attempt 2
        clock.advanceMillis(200);
        relay2.runOnce(); // attempt 3 -> dead
        relay2.close();

        assertThat(persisted.status()).isEqualTo(MessageStatus.DEAD);
        assertThat(calls).hasValue(3);
    }

    private static String enqueue(TransactionalDatabase db, String key) {
        final String[] id = new String[1];
        db.inTransaction(tx -> {
            OutboxMessage m = tx.enqueue(key, "OrderCreated", "{}");
            id[0] = m.id();
            tx.putBusiness(key, "CREATED");
        });
        return id[0];
    }
}
