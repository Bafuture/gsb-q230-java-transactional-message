package com.example.gsb.txmsg;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** In-memory implementation of the business data table. */
public final class InMemoryBusinessStore implements BusinessStore {

    private final Map<String, String> data = new LinkedHashMap<>();

    @Override
    public synchronized void put(String key, String value) {
        data.put(key, value);
    }

    @Override
    public synchronized Optional<String> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    @Override
    public synchronized Map<String, String> snapshot() {
        return new LinkedHashMap<>(data);
    }

    @Override
    public synchronized void restore(Map<String, String> state) {
        data.clear();
        data.putAll(state);
    }
}
