package com.example.txmsg;

/** Manually advanced clock for deterministic backoff tests. */
final class FakeClock implements Clock {

    private long now;

    FakeClock(long startMillis) {
        this.now = startMillis;
    }

    @Override
    public long nowMillis() {
        return now;
    }

    void advanceMillis(long delta) {
        now += delta;
    }
}
