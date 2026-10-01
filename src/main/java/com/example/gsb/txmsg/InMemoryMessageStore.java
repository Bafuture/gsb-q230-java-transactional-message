package com.example.gsb.txmsg;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** In-memory implementation of the local message table. */
public final class InMemoryMessageStore implements MessageStore {

    private final Map<String, MessageEnvelope> messages = new LinkedHashMap<>();

    @Override
    public synchronized void put(MessageEnvelope message) {
        messages.put(message.getId(), message);
    }

    @Override
    public synchronized Optional<MessageEnvelope> findById(String id) {
        return Optional.ofNullable(messages.get(id));
    }

    @Override
    public synchronized List<MessageEnvelope> findAll() {
        return new ArrayList<>(messages.values());
    }

    @Override
    public synchronized void restore(List<MessageEnvelope> state) {
        messages.clear();
        for (MessageEnvelope message : state) {
            messages.put(message.getId(), message);
        }
    }
}
