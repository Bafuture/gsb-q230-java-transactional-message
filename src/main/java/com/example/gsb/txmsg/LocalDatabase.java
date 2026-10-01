package com.example.gsb.txmsg;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Coordinates the two in-memory stores ("business table" and "local message
 * table") behind a single local transaction and an optional durable snapshot.
 *
 * <p>Transaction semantics: all {@link TransactionContext} operations are
 * staged while the action runs. The staged changes become visible in both
 * stores atomically when the action returns normally; if the action throws,
 * nothing is visible afterwards. When a storage directory is configured the
 * combined snapshot is written atomically on every commit and on every
 * delivery-state change, which makes the state recoverable after a restart.
 */
public final class LocalDatabase {

    private final Object mutex = new Object();
    private final InMemoryBusinessStore businessStore = new InMemoryBusinessStore();
    private final InMemoryMessageStore messageStore = new InMemoryMessageStore();
    private final Path storageFile;
    private long sequence;

    private LocalDatabase(Path storageFile) {
        this.storageFile = storageFile;
    }

    /** Creates a non-durable database (tests / demos only). */
    public static LocalDatabase inMemory() {
        return new LocalDatabase(null);
    }

    /**
     * Opens (or creates) a durable database backed by
     * {@code <directory>/local-database.bin}.
     */
    public static LocalDatabase fileBacked(Path directory) {
        Objects.requireNonNull(directory, "directory");
        LocalDatabase database = new LocalDatabase(directory.resolve("local-database.bin"));
        database.restoreFromDisk();
        return database;
    }

    /**
     * Runs {@code action} inside one local transaction. Business writes and
     * outgoing messages either both commit or both roll back.
     */
    public void inTransaction(Consumer<TransactionContext> action) {
        Objects.requireNonNull(action, "action");
        synchronized (mutex) {
            StagingTransactionContext staging = new StagingTransactionContext();
            action.accept(staging);

            Map<String, String> previousBusiness = businessStore.snapshot();
            List<MessageEnvelope> previousMessages = messageStore.findAll();

            staging.businessWrites.forEach(businessStore::put);
            staging.messages.forEach(messageStore::put);

            try {
                persistLocked();
            } catch (TransactionException persistFailure) {
                businessStore.restore(previousBusiness);
                messageStore.restore(previousMessages);
                throw persistFailure;
            }
        }
    }

    /** Persists the SENT state of a successfully delivered message. */
    void markSent(String messageId, long startedAtMillis, long finishedAtMillis) {
        synchronized (mutex) {
            MessageEnvelope message = liveMessage(messageId);
            message.recordAttempt(true, startedAtMillis, finishedAtMillis, null);
            message.markSent(finishedAtMillis);
            persistLocked();
        }
    }

    /** Records a failed delivery and schedules retry or dead-lettering. */
    void recordFailure(String messageId, RetryPolicy retryPolicy,
                       long startedAtMillis, long finishedAtMillis, String error) {
        synchronized (mutex) {
            MessageEnvelope message = liveMessage(messageId);
            message.recordAttempt(false, startedAtMillis, finishedAtMillis, error);
            if (message.getAttempts() >= retryPolicy.maxAttempts()) {
                message.markDeadLetter(finishedAtMillis,
                        "Delivery failed after " + message.getAttempts() + " attempt(s); last error: " + error);
            } else {
                long backoffMillis = retryPolicy.backoffForAttempt(message.getAttempts());
                message.scheduleRetry(finishedAtMillis, backoffMillis);
            }
            persistLocked();
        }
    }

    /**
     * Re-enqueues a SENT or DEAD_LETTER message for delivery. Used for manual
     * handling of dead letters and to simulate redelivery after a crash.
     */
    public void requeue(String messageId) {
        synchronized (mutex) {
            MessageEnvelope message = liveMessage(messageId);
            message.requeue(System.currentTimeMillis());
            persistLocked();
        }
    }

    public Optional<String> businessData(String key) {
        synchronized (mutex) {
            return businessStore.get(key);
        }
    }

    public Map<String, String> businessData() {
        synchronized (mutex) {
            return businessStore.snapshot();
        }
    }

    /** Returns defensive copies of all messages, ordered by sequence. */
    public List<MessageEnvelope> messages() {
        synchronized (mutex) {
            List<MessageEnvelope> copies = new ArrayList<>();
            for (MessageEnvelope message : messageStore.findAll()) {
                copies.add(message.copy());
            }
            copies.sort((left, right) -> Long.compare(left.getSequence(), right.getSequence()));
            return copies;
        }
    }

    public Optional<MessageEnvelope> message(String id) {
        synchronized (mutex) {
            return messageStore.findById(id).map(MessageEnvelope::copy);
        }
    }

    private MessageEnvelope liveMessage(String messageId) {
        return messageStore.findById(messageId)
                .orElseThrow(() -> new NoSuchElementException("Unknown message: " + messageId));
    }

    private void persistLocked() {
        if (storageFile == null) {
            return;
        }
        Snapshot snapshot = new Snapshot(businessStore.snapshot(), messageStore.findAll());
        Path tempFile = storageFile.resolveSibling(storageFile.getFileName() + ".tmp");
        try {
            Files.createDirectories(storageFile.getParent());
            try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(tempFile))) {
                out.writeObject(snapshot);
            }
            try {
                Files.move(tempFile, storageFile,
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempFile, storageFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new TransactionException("Failed to persist local database to " + storageFile, e);
        }
    }

    private void restoreFromDisk() {
        if (storageFile == null || !Files.exists(storageFile)) {
            return;
        }
        try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(storageFile))) {
            Snapshot snapshot = (Snapshot) in.readObject();
            synchronized (mutex) {
                businessStore.restore(snapshot.business);
                messageStore.restore(snapshot.messages);
                sequence = snapshot.messages.stream()
                        .mapToLong(MessageEnvelope::getSequence)
                        .max()
                        .orElse(0L);
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new TransactionException("Failed to load local database from " + storageFile, e);
        }
    }

    /** Staging area for one transaction; nothing touches the real stores on rollback. */
    private final class StagingTransactionContext implements TransactionContext {
        private final Map<String, String> businessWrites = new LinkedHashMap<>();
        private final List<MessageEnvelope> messages = new ArrayList<>();

        @Override
        public void putBusinessData(String key, String value) {
            businessWrites.put(Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
        }

        @Override
        public String send(String businessKey, String payload) {
            Objects.requireNonNull(businessKey, "businessKey");
            Objects.requireNonNull(payload, "payload");
            MessageEnvelope message = new MessageEnvelope(
                    UUID.randomUUID().toString(), businessKey, payload,
                    ++sequence, System.currentTimeMillis());
            messages.add(message);
            return message.getId();
        }
    }

    /** Combined durable snapshot of both stores. */
    private static final class Snapshot implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Map<String, String> business;
        private final List<MessageEnvelope> messages;

        Snapshot(Map<String, String> business, List<MessageEnvelope> messages) {
            this.business = new LinkedHashMap<>(business);
            this.messages = new ArrayList<>(messages);
        }
    }
}
