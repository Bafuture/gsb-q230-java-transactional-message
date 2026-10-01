package com.example.gsb.txmsg;

import java.util.List;
import java.util.Optional;

/** Store simulating the local message table. */
public interface MessageStore {

    /** Inserts a new message or replaces an existing one with the same ID. */
    void put(MessageEnvelope message);

    Optional<MessageEnvelope> findById(String id);

    /** Returns all messages in insertion (sequence) order. */
    List<MessageEnvelope> findAll();

    /** Replaces the whole contents (used for restore and rollback). */
    void restore(List<MessageEnvelope> messages);
}
