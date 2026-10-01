package com.example.txmsg;

/**
 * Downstream endpoint the relay delivers messages to. Implementations may
 * throw to signal a transient delivery failure; the relay will retry with
 * backoff.
 */
@FunctionalInterface
public interface MessageConsumer {

    void consume(OutboxMessage message) throws Exception;
}
