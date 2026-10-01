package com.example.txmsg;

import java.util.ArrayList;
import java.util.List;

/**
 * Point-in-time statistics over the message table: status counts, delivery
 * effort (attempts / retries / failures) and per-state durations.
 */
public record OutboxStats(
        long pendingCount,
        long sentCount,
        long deadCount,
        long totalAttempts,
        long totalRetries,
        long totalFailures,
        TimingStats pendingAge,
        TimingStats sentLatency,
        TimingStats deadLatency) {

    /**
     * Computes a snapshot from the store at {@code nowMillis}.
     *
     * <ul>
     *   <li>{@code totalRetries}: delivery attempts after the first one, summed over all messages</li>
     *   <li>{@code totalFailures}: attempts that ended in an exception (each retry is a failure)</li>
     *   <li>{@code pendingAge}: how long still-pending messages have been waiting</li>
     *   <li>{@code sentLatency}: enqueue-to-acknowledged time of sent messages</li>
     *   <li>{@code deadLatency}: enqueue-to-dead-lettered time of dead messages</li>
     * </ul>
     */
    public static OutboxStats snapshot(MessageStore store, long nowMillis) {
        long pending = 0;
        long sent = 0;
        long dead = 0;
        long attempts = 0;
        long retries = 0;
        long failures = 0;
        List<Long> pendingAges = new ArrayList<>();
        List<Long> sentLatencies = new ArrayList<>();
        List<Long> deadLatencies = new ArrayList<>();

        for (OutboxMessage m : store.snapshot()) {
            attempts += m.attempts();
            retries += Math.max(0, m.attempts() - 1);
            switch (m.status()) {
                case PENDING -> {
                    pending++;
                    pendingAges.add(nowMillis - m.createdAtMillis());
                    failures += m.attempts();
                }
                case SENT -> {
                    sent++;
                    sentLatencies.add(m.updatedAtMillis() - m.createdAtMillis());
                    failures += Math.max(0, m.attempts() - 1);
                }
                case DEAD -> {
                    dead++;
                    deadLatencies.add(m.updatedAtMillis() - m.createdAtMillis());
                    failures += m.attempts();
                }
            }
        }
        return new OutboxStats(pending, sent, dead, attempts, retries, failures,
                TimingStats.of(pendingAges), TimingStats.of(sentLatencies), TimingStats.of(deadLatencies));
    }
}
