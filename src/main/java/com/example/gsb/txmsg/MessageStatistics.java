package com.example.gsb.txmsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Computes {@link StatisticsSnapshot}s from the local message table. */
public final class MessageStatistics {

    private final LocalDatabase database;

    public MessageStatistics(LocalDatabase database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    public StatisticsSnapshot snapshot() {
        List<MessageEnvelope> messages = database.messages();
        long now = System.currentTimeMillis();

        long pending = 0;
        long sent = 0;
        long deadLetter = 0;
        long attempts = 0;
        long retries = 0;
        List<Long> sendLatency = new ArrayList<>();
        List<Long> pendingAge = new ArrayList<>();
        List<Long> deadLetterLatency = new ArrayList<>();
        List<Long> backoffWait = new ArrayList<>();

        for (MessageEnvelope message : messages) {
            attempts += message.getAttempts();
            retries += Math.max(0, message.getAttempts() - 1);
            if (message.getTotalBackoffMillis() > 0) {
                backoffWait.add(message.getTotalBackoffMillis());
            }
            switch (message.getStatus()) {
                case PENDING -> {
                    pending++;
                    pendingAge.add(now - message.getCreatedAtMillis());
                }
                case SENT -> {
                    sent++;
                    sendLatency.add(message.getSentAtMillis() - message.getCreatedAtMillis());
                }
                case DEAD_LETTER -> {
                    deadLetter++;
                    deadLetterLatency.add(message.getDeadLetteredAtMillis() - message.getCreatedAtMillis());
                }
            }
        }

        return new StatisticsSnapshot(
                messages.size(), pending, sent, deadLetter,
                attempts, retries,
                DurationStats.of(sendLatency),
                DurationStats.of(pendingAge),
                DurationStats.of(deadLetterLatency),
                DurationStats.of(backoffWait));
    }
}
