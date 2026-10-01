package com.example.gsb.txmsg;

import static org.assertj.core.api.Assertions.fail;

import java.util.function.BooleanSupplier;

/** Tiny polling helper so tests do not depend on Awaitility. */
final class Await {

    private static final long DEFAULT_TIMEOUT_MILLIS = 10_000;
    private static final long POLL_INTERVAL_MILLIS = 10;

    private Await() {
    }

    static void until(String description, BooleanSupplier condition) {
        until(description, condition, DEFAULT_TIMEOUT_MILLIS);
    }

    static void until(String description, BooleanSupplier condition, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (true) {
            boolean satisfied;
            try {
                satisfied = condition.getAsBoolean();
            } catch (RuntimeException ignored) {
                satisfied = false;
            }
            if (satisfied) {
                return;
            }
            if (System.currentTimeMillis() >= deadline) {
                fail("Timed out after %d ms waiting for: %s", timeoutMillis, description);
            }
            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for: %s", description);
            }
        }
    }
}
