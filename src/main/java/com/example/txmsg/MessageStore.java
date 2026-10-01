package com.example.txmsg;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The local message table. Messages are staged by the business transaction
 * and appended atomically at commit. Delivery state is mutated only through
 * this class (callers must hold its monitor), so reads observe a consistent
 * snapshot of the table.
 */
public final class MessageStore {

    private final ConcurrentMap<String, OutboxMessage> byId = new ConcurrentHashMap<>();
    private final List<OutboxMessage> ordered = new ArrayList<>();
    private final AtomicLong sequenceGenerator = new AtomicLong(0);

    private final List<OutboxMessage> staged = new ArrayList<>();

    void begin() {
        staged.clear();
    }

    /** Called inside a transaction; assigns the message's production order at commit. */
    void stage(OutboxMessage message) {
        staged.add(message);
    }

    void commit(long nowMillis) {
        // Appended in the order the business code enqueued them, which also defines
        // per-business-key delivery order.
        for (OutboxMessage message : staged) {
            long seq = sequenceGenerator.incrementAndGet();
            message.assignCommitMetadata(seq, nowMillis);
            ordered.add(message);
            byId.put(message.id(), message);
        }
        resetStage();
    }

    void rollback() {
        resetStage();
    }

    private void resetStage() {
        staged.clear();
    }

    public OutboxMessage find(String id) {
        return byId.get(id);
    }

    /** Immutable view of every committed message, in production order. */
    public List<OutboxMessage> snapshot() {
        synchronized (this) {
            return List.copyOf(ordered);
        }
    }

    /**
     * Messages that may be delivered now: due for retry and not blocked by an
     * earlier, not-yet-finished message of the same business key (which would
     * violate per-key ordering). A dead earlier message blocks later ones:
     * delivery must not silently overtake a message awaiting manual handling.
     */
    public List<OutboxMessage> dueMessages(long nowMillis) {
        synchronized (this) {
            List<OutboxMessage> due = new ArrayList<>();
            for (OutboxMessage candidate : ordered) {
                if (candidate.status() != MessageStatus.PENDING
                        || candidate.nextAttemptAtMillis() > nowMillis) {
                    continue;
                }
                boolean blockedByEarlier = false;
                for (OutboxMessage earlier : ordered) {
                    if (earlier == candidate) {
                        break;
                    }
                    if (earlier.businessKey().equals(candidate.businessKey())
                            && earlier.status() != MessageStatus.SENT) {
                        blockedByEarlier = true;
                        break;
                    }
                }
                if (!blockedByEarlier) {
                    due.add(candidate);
                }
            }
            return due;
        }
    }

    void markSent(OutboxMessage message, long nowMillis) {
        synchronized (this) {
            message.markSent(nowMillis);
        }
    }

    void scheduleRetry(OutboxMessage message, long nowMillis, long nextAttemptAtMillis, String error) {
        synchronized (this) {
            message.scheduleRetry(nowMillis, nextAttemptAtMillis, error);
        }
    }

    void markDead(OutboxMessage message, long nowMillis, String error) {
        synchronized (this) {
            message.markDead(nowMillis, error);
        }
    }

    long countByStatus(MessageStatus status) {
        synchronized (this) {
            long n = 0;
            for (OutboxMessage m : ordered) {
                if (m.status() == status) {
                    n++;
                }
            }
            return n;
        }
    }

    // ------------------------------------------------------------------
    // Restart simulation
    //
    // The table is "durable": its committed content survives replacement of
    // the relay process. In a real system this would be a database table;
    // here the store is the durable medium shared by relay instances.
    // ------------------------------------------------------------------

    /** Highest durable sequence; used after recovery to continue numbering without collision. */
    long maxSequence() {
        synchronized (this) {
            return sequenceGenerator.get();
        }
    }

    void adoptDurableSequenceAfter(long sequence) {
        sequenceGenerator.updateAndGet(current -> Math.max(current, sequence));
    }

    /** For diagnostics. */
    Map<String, Long> statusCounts(long nowMillis) {
        synchronized (this) {
            long pending = 0;
            long sent = 0;
            long dead = 0;
            for (OutboxMessage m : ordered) {
                switch (m.status()) {
                    case PENDING -> pending++;
                    case SENT -> sent++;
                    case DEAD -> dead++;
                }
            }
            Comparator<OutboxMessage> c = Comparator.comparingLong(OutboxMessage::sequence);
            return Map.of("pending", pending, "sent", sent, "dead", dead,
                    "total", (long) ordered.size());
        }
    }
}
