package com.example.txmsg;

import java.util.List;

/** Aggregate of a set of durations in milliseconds. */
public record TimingStats(long count, long totalMillis, long minMillis, long maxMillis) {

    public static final TimingStats EMPTY = new TimingStats(0, 0, 0, 0);

    public double averageMillis() {
        return count == 0 ? 0.0 : (double) totalMillis / count;
    }

    public static TimingStats of(List<Long> durations) {
        if (durations.isEmpty()) {
            return EMPTY;
        }
        long total = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long d : durations) {
            total += d;
            min = Math.min(min, d);
            max = Math.max(max, d);
        }
        return new TimingStats(durations.size(), total, min, max);
    }
}
