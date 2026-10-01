package com.example.txmsg;

import java.util.UUID;

/**
 * Handle to the currently running transaction. Business code uses it to write
 * business data and enqueue outbox messages; both become visible atomically
 * when the transaction commits, or not at all when it rolls back.
 */
public final class TxContext {

    private final TransactionalDatabase database;

    TxContext(TransactionalDatabase database) {
        this.database = database;
    }

    /** Stage a business write; visible to readers only after commit. */
    public void putBusiness(String key, String value) {
        database.businessStore().stagePut(
                new BusinessData(key, value, database.clock().nowMillis()));
    }

    /** Read business data, seeing this transaction's own staged write. */
    public BusinessData getBusiness(String key) {
        return database.businessStore().find(key);
    }

    /** Enqueue an outbox message; appended to the message table only at commit. */
    public OutboxMessage enqueue(String businessKey, String type, String payload) {
        OutboxMessage message = new OutboxMessage(UUID.randomUUID().toString(), businessKey, type, payload);
        database.messageStore().stage(message);
        return message;
    }
}
