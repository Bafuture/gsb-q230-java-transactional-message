package com.example.txmsg;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * In-memory store for business data. Uncommitted writes are kept in the
 * per-transaction staging map and become visible atomically at commit.
 */
public final class BusinessStore {

    private final Map<String, BusinessData> committed = new LinkedHashMap<>();
    private BusinessData staged;
    private boolean touched;

    void begin() {
        staged = null;
        touched = false;
    }

    void stagePut(BusinessData data) {
        staged = data;
        touched = true;
    }

    BusinessData staged(String key) {
        return staged != null && staged.key().equals(key) ? staged : null;
    }

    void commit() {
        if (touched) {
            committed.put(staged.key(), staged);
        }
        resetStage();
    }

    void rollback() {
        resetStage();
    }

    private void resetStage() {
        staged = null;
        touched = false;
    }

    public BusinessData find(String key) {
        BusinessData s = staged(key);
        return s != null ? s : committed.get(key);
    }

    public boolean exists(String key) {
        return find(key) != null;
    }

    public long size() {
        return committed.size();
    }
}
