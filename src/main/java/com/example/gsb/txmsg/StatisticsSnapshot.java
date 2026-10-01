package com.example.gsb.txmsg;

/**
 * Point-in-time statistics of the local message table.
 *
 * @param totalCount            all messages ever written
 * @param pendingCount          messages waiting to be (re)delivered
 * @param sentCount             messages acknowledged by the downstream
 * @param deadLetterCount       messages that exhausted retries (manual handling)
 * @param totalDeliveryAttempts delivery attempts across all messages
 * @param totalRetries          retries across all messages (attempts beyond the first)
 * @param sendLatency           time from creation to SENT, per sent message
 * @param pendingAge            current age of pending messages
 * @param deadLetterLatency     time from creation to dead-lettering
 * @param backoffWait           accumulated backoff wait per retried message
 */
public record StatisticsSnapshot(
        long totalCount,
        long pendingCount,
        long sentCount,
        long deadLetterCount,
        long totalDeliveryAttempts,
        long totalRetries,
        DurationStats sendLatency,
        DurationStats pendingAge,
        DurationStats deadLetterLatency,
        DurationStats backoffWait
) {
}
