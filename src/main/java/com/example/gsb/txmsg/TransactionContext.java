package com.example.gsb.txmsg;

/**
 * Operations available inside a local transaction.
 *
 * <p>Business data writes ({@link #putBusinessData}) and outgoing messages
 * ({@link #send}) are staged during the transaction and committed together:
 * if the action throws, nothing staged is visible afterwards.
 */
public interface TransactionContext {

    /** Writes one business record (upsert by key) as part of the transaction. */
    void putBusinessData(String key, String value);

    /**
     * Stages one outgoing message as part of the transaction.
     *
     * @param businessKey key used for strict per-key delivery ordering
     * @param payload     opaque message body handed to the downstream endpoint
     * @return the generated unique message ID
     */
    String send(String businessKey, String payload);
}
