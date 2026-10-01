package com.example.txmsg;

/**
 * Time source used by the outbox and relay, so retry backoff can be
 * driven deterministically in tests.
 */
@FunctionalInterface
public interface Clock {

    long nowMillis();

    Clock SYSTEM = System::currentTimeMillis;

    static Clock system() {
        return SYSTEM;
    }
}
