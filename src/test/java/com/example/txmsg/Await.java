package com.example.txmsg;

import java.util.function.BooleanSupplier;

/** Minimal polling await helper for asynchronous assertions. */
final class Await {

    private Await() {
    }

    static void until(String description, BooleanSupplier condition) {
        until(description, 5000, condition);
    }

    static void until(String description, long timeoutMillis, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for: " + description);
            }
        }
        throw new AssertionError("timed out waiting for: " + description);
    }
}
