package com.example.gsb.txmsg;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Idempotent consumer wrapper: applies each message ID at most once, even if
 * the same message is delivered repeatedly (at-least-once delivery). The set
 * of applied message IDs can be persisted so deduplication survives restarts.
 *
 * <p>Duplicates are silently acknowledged so the dispatcher can mark them
 * SENT instead of retrying forever.
 */
public final class IdempotentConsumer implements DownstreamEndpoint {

    private final Object mutex = new Object();
    private final DownstreamEndpoint delegate;
    private final Path stateFile;
    private final Set<String> appliedMessageIds;

    private IdempotentConsumer(DownstreamEndpoint delegate, Path stateFile) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.stateFile = stateFile;
        this.appliedMessageIds = load(stateFile);
    }

    /** In-memory deduplication only. */
    public static IdempotentConsumer inMemory(DownstreamEndpoint delegate) {
        return new IdempotentConsumer(delegate, null);
    }

    /** Durable deduplication backed by the given state file. */
    public static IdempotentConsumer fileBacked(Path stateFile, DownstreamEndpoint delegate) {
        return new IdempotentConsumer(delegate, stateFile);
    }

    @Override
    public void deliver(MessageEnvelope message) throws Exception {
        synchronized (mutex) {
            if (appliedMessageIds.contains(message.getId())) {
                return;
            }
            delegate.deliver(message);
            appliedMessageIds.add(message.getId());
            persistLocked();
        }
    }

    public boolean hasApplied(String messageId) {
        synchronized (mutex) {
            return appliedMessageIds.contains(messageId);
        }
    }

    public int appliedCount() {
        synchronized (mutex) {
            return appliedMessageIds.size();
        }
    }

    private void persistLocked() {
        if (stateFile == null) {
            return;
        }
        Path tempFile = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
        try {
            Files.createDirectories(stateFile.getParent());
            try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(tempFile))) {
                out.writeObject(new HashSet<>(appliedMessageIds));
            }
            try {
                Files.move(tempFile, stateFile,
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new TransactionException("Failed to persist consumer state to " + stateFile, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<String> load(Path stateFile) {
        if (stateFile == null || !Files.exists(stateFile)) {
            return new HashSet<>();
        }
        try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(stateFile))) {
            return (Set<String>) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new TransactionException("Failed to load consumer state from " + stateFile, e);
        }
    }
}
