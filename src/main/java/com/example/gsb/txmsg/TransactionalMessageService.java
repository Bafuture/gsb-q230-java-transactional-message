package com.example.gsb.txmsg;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Facade tying the local transaction, the background dispatcher and the
 * statistics together.
 *
 * <pre>{@code
 * LocalDatabase db = LocalDatabase.fileBacked(Path.of("data"));
 * try (TransactionalMessageService service =
 *         new TransactionalMessageService(db, endpoint, RetryPolicy.exponential(5, 1000), 100)) {
 *     service.start();
 *     service.inTransaction(ctx -> {
 *         ctx.putBusinessData("order-1", "CREATED");
 *         ctx.send("order-1", "{\"event\":\"order-created\"}");
 *     });
 * }
 * }</pre>
 */
public final class TransactionalMessageService implements AutoCloseable {

    private final LocalDatabase database;
    private final MessageDispatcher dispatcher;
    private final MessageStatistics statistics;

    public TransactionalMessageService(LocalDatabase database, DownstreamEndpoint endpoint,
                                       RetryPolicy retryPolicy, long pollIntervalMillis) {
        this.database = Objects.requireNonNull(database, "database");
        this.dispatcher = new MessageDispatcher(database, endpoint, retryPolicy, pollIntervalMillis);
        this.statistics = new MessageStatistics(database);
    }

    /** Writes business data and outgoing messages in one atomic local transaction. */
    public void inTransaction(Consumer<TransactionContext> action) {
        database.inTransaction(action);
    }

    /** Starts background delivery. */
    public void start() {
        dispatcher.start();
    }

    /** Runs one delivery round synchronously. */
    public void pollOnce() {
        dispatcher.pollOnce();
    }

    /** Re-enqueues a SENT or DEAD_LETTER message (manual recovery). */
    public void requeue(String messageId) {
        database.requeue(messageId);
    }

    public StatisticsSnapshot statistics() {
        return statistics.snapshot();
    }

    public LocalDatabase database() {
        return database;
    }

    public MessageDispatcher dispatcher() {
        return dispatcher;
    }

    /** Stops background delivery. */
    @Override
    public void close() {
        dispatcher.close();
    }
}
