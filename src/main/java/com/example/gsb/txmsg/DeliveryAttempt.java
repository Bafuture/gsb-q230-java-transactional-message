package com.example.gsb.txmsg;

import java.io.Serializable;

/**
 * One delivery attempt against the downstream endpoint.
 *
 * @param attemptNumber    1-based attempt number
 * @param startedAtMillis  wall-clock time the attempt started
 * @param durationMillis   wall-clock duration of the attempt
 * @param success          whether the downstream accepted the message
 * @param error            stringified failure when {@code success} is false
 */
public record DeliveryAttempt(
        int attemptNumber,
        long startedAtMillis,
        long durationMillis,
        boolean success,
        String error
) implements Serializable {
    private static final long serialVersionUID = 1L;
}
