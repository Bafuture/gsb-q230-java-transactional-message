package com.example.gsb.txmsg;

/**
 * Destination of transactional messages. Implementations apply the message
 * effect (e.g. publish to a broker, call an API) and signal failure by
 * throwing any exception.
 */
@FunctionalInterface
public interface DownstreamEndpoint {

    /**
     * Delivers one message. Throwing means the delivery failed and the
     * message will be retried according to the configured {@link RetryPolicy}.
     */
    void deliver(MessageEnvelope message) throws Exception;
}
