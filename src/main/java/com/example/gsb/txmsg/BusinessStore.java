package com.example.gsb.txmsg;

import java.util.Map;
import java.util.Optional;

/** Store simulating the business data table. */
public interface BusinessStore {

    void put(String key, String value);

    Optional<String> get(String key);

    /** Returns a detached copy of the current contents. */
    Map<String, String> snapshot();

    /** Replaces the whole contents (used for restore and rollback). */
    void restore(Map<String, String> state);
}
