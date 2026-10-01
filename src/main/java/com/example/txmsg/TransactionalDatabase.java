package com.example.txmsg;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Simulated database holding the business store and the local message table.
 * {@link #inTransaction(Consumer)} gives both stores a single atomic commit
 * point: staged writes become visible together, or are discarded together.
 *
 * <p>Transactions are serialized by a global lock, which keeps the two-store
 * commit atomic and the message sequence assignment consistent.
 */
public final class TransactionalDatabase {

    private final Object txLock = new Object();
    private final BusinessStore businessStore;
    private final MessageStore messageStore;
    private final Clock clock;

    public TransactionalDatabase() {
        this(new BusinessStore(), new MessageStore(), Clock.system());
    }

    public TransactionalDatabase(BusinessStore businessStore, MessageStore messageStore, Clock clock) {
        this.businessStore = Objects.requireNonNull(businessStore);
        this.messageStore = Objects.requireNonNull(messageStore);
        this.clock = Objects.requireNonNull(clock);
    }

    BusinessStore businessStore() {
        return businessStore;
    }

    public MessageStore messageStore() {
        return messageStore;
    }

    Clock clock() {
        return clock;
    }

    /**
     * Runs {@code work} in one local transaction. If {@code work} throws, all
     * staged business writes and outbox messages are rolled back; otherwise
     * they are committed atomically before this method returns.
     */
    public void inTransaction(Consumer<TxContext> work) {
        synchronized (txLock) {
            businessStore.begin();
            messageStore.begin();
            TxContext tx = new TxContext(this);
            try {
                work.accept(tx);
            } catch (RuntimeException | Error e) {
                businessStore.rollback();
                messageStore.rollback();
                throw e;
            }
            long now = clock.nowMillis();
            businessStore.commit();
            messageStore.commit(now);
        }
    }
}
