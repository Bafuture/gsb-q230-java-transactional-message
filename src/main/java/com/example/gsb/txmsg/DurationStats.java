package com.example.gsb.txmsg;

import java.util.List;

/**
 * Summary of a set of durations in milliseconds.
 */
public record DurationStats(long count, long totalMillis, long minMillis, long maxMillis, double averageMillis) {

    public static final DurationStats EMPTY = new DurationStats(0, 0, 0, 0, 0.0);

    public static DurationStats of(List<Long> values) {
        if (values.isEmpty()) {
            return EMPTY;
        }
        long total = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long value : values) {
            total += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return new DurationStats(values.size(), total, min, max, (double) total / values.size());
    }
}
