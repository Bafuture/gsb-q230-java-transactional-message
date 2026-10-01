package com.example.txmsg;

import java.util.Objects;

/** A piece of business data persisted together with an outbox message. */
public final class BusinessData {

    private final String key;
    private final String value;
    private final long createdAtMillis;

    public BusinessData(String key, String value, long createdAtMillis) {
        this.key = Objects.requireNonNull(key, "key");
        this.value = Objects.requireNonNull(value, "value");
        this.createdAtMillis = createdAtMillis;
    }

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }
}
