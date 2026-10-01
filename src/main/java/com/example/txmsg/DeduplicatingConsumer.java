package com.example.txmsg;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Idempotent consumer wrapper: delivery is at-least-once, so the same message
 * id may arrive more than once. The wrapped effect runs at most once per id;
 * duplicates are acknowledged without re-applying the effect.
 */
public final class DeduplicatingConsumer implements MessageConsumer {

    private final Consumer<OutboxMessage> effect;
    private final Set<String> appliedIds = ConcurrentHashMap.newKeySet();
    private final List<OutboxMessage> applied = new CopyOnWriteArrayList<>();
    private final List<OutboxMessage> received = new CopyOnWriteArrayList<>();

    public DeduplicatingConsumer(Consumer<OutboxMessage> effect) {
        this.effect = effect;
    }

    @Override
    public void consume(OutboxMessage message) {
        received.add(message);
        if (appliedIds.add(message.id())) {
            effect.accept(message);
            applied.add(message);
        }
        // Duplicate id: already applied, acknowledge without re-applying.
    }

    /** Messages whose effect was actually applied (at most once per id). */
    public List<OutboxMessage> appliedMessages() {
        return List.copyOf(applied);
    }

    /** Every delivery attempt seen, including duplicates. */
    public List<OutboxMessage> receivedMessages() {
        return List.copyOf(received);
    }

    public boolean hasApplied(String messageId) {
        return appliedIds.contains(messageId);
    }

    public Map<String, Integer> appliedCountById() {
        Map<String, Integer> counts = new ConcurrentHashMap<>();
        for (OutboxMessage m : applied) {
            counts.merge(m.id(), 1, Integer::sum);
        }
        return counts;
    }
}
