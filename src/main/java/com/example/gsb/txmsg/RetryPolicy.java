package com.example.gsb.txmsg;

import java.time.Duration;

/**
 * Retry policy: up to {@code maxAttempts} delivery attempts, with
 * exponential backoff between attempts.
 *
 * @param maxAttempts      total attempts before dead-lettering (>= 1)
 * @param baseBackoffMillis backoff after the first failed attempt (>= 0)
 * @param multiplier       growth factor per failed attempt (>= 1.0)
 */
public record RetryPolicy(int maxAttempts, long baseBackoffMillis, double multiplier) {

    private static final long MAX_BACKOFF_MILLIS = Duration.ofHours(1).toMillis();

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (baseBackoffMillis < 0) {
            throw new IllegalArgumentException("baseBackoffMillis must be >= 0");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
    }

    /** Exponential backoff with base 2. */
    public static RetryPolicy exponential(int maxAttempts, long baseBackoffMillis) {
        return new RetryPolicy(maxAttempts, baseBackoffMillis, 2.0);
    }

    /** Backoff to wait after the given number of consecutive failures (1-based). */
    public long backoffForAttempt(int failedAttemptCount) {
        double backoff = baseBackoffMillis * Math.pow(multiplier, failedAttemptCount - 1.0);
        return Math.min(MAX_BACKOFF_MILLIS, Math.round(backoff));
    }
}
