package com.example.gsb.txmsg;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/** Recording downstream endpoint with scriptable failures. */
final class TestEndpoint implements DownstreamEndpoint {

    private final List<MessageEnvelope> deliveries = new CopyOnWriteArrayList<>();
    private final Map<String, Integer> attemptsById = new ConcurrentHashMap<>();
    private final AtomicInteger totalCalls = new AtomicInteger();
    private volatile Predicate<MessageEnvelope> failure = message -> false;

    /** The predicate is evaluated after the attempt counter is incremented. */
    void failWhen(Predicate<MessageEnvelope> failure) {
        this.failure = failure;
    }

    @Override
    public void deliver(MessageEnvelope message) {
        totalCalls.incrementAndGet();
        attemptsById.merge(message.getId(), 1, Integer::sum);
        if (failure.test(message)) {
            throw new IllegalStateException(
                    "simulated downstream failure for payload '" + message.getPayload() + "'");
        }
        deliveries.add(message);
    }

    int attemptsFor(String messageId) {
        return attemptsById.getOrDefault(messageId, 0);
    }

    int totalCalls() {
        return totalCalls.get();
    }

    List<MessageEnvelope> deliveries() {
        return List.copyOf(deliveries);
    }

    List<String> deliveredPayloads(String businessKey) {
        return deliveries.stream()
                .filter(message -> message.getBusinessKey().equals(businessKey))
                .map(MessageEnvelope::getPayload)
                .toList();
    }
}
