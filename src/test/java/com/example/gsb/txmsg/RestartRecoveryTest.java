package com.example.gsb.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Requirement 5: pending work resumes after restart; completed work is never redelivered. */
class RestartRecoveryTest {

    @TempDir
    Path directory;

    @Test
    void pendingMessagesResumeAndSentMessagesAreNotRedelivered() {
        LocalDatabase first = LocalDatabase.fileBacked(directory);
        first.inTransaction(ctx -> {
            ctx.putBusinessData("acc-1", "balance=90");
            ctx.send("acc-1", "m1");
            ctx.send("acc-1", "m2");
        });

        TestEndpoint firstEndpoint = new TestEndpoint();
        MessageDispatcher firstDispatcher =
                new MessageDispatcher(first, firstEndpoint, RetryPolicy.exponential(3, 10), 10);
        firstDispatcher.pollOnce(); // only m1 is eligible; same-key m2 stays behind
        assertThat(firstEndpoint.deliveries()).hasSize(1);
        // The process "crashes": no graceful shutdown, state lives only on disk.

        LocalDatabase second = LocalDatabase.fileBacked(directory);
        assertThat(second.businessData()).containsEntry("acc-1", "balance=90");
        assertThat(second.messages()).hasSize(2);
        assertThat(second.messages().get(0).getStatus()).isEqualTo(MessageStatus.SENT);
        assertThat(second.messages().get(1).getStatus()).isEqualTo(MessageStatus.PENDING);

        TestEndpoint secondEndpoint = new TestEndpoint();
        MessageDispatcher secondDispatcher =
                new MessageDispatcher(second, secondEndpoint, RetryPolicy.exponential(3, 10), 10);
        secondDispatcher.pollOnce(); // resumes m2; m1 must not be redelivered

        assertThat(secondEndpoint.deliveries())
                .extracting(MessageEnvelope::getPayload)
                .containsExactly("m2");
        assertThat(firstEndpoint.deliveries())
                .extracting(MessageEnvelope::getPayload)
                .containsExactly("m1");
        assertThat(second.messages().get(1).getStatus()).isEqualTo(MessageStatus.SENT);

        MessageStatistics statistics = new MessageStatistics(second);
        assertThat(statistics.snapshot().sentCount()).isEqualTo(2);
        assertThat(statistics.snapshot().pendingCount()).isZero();
    }

    @Test
    void deadLetterStateAndRetryHistorySurviveRestart() throws Exception {
        LocalDatabase first = LocalDatabase.fileBacked(directory);
        TestEndpoint failingEndpoint = new TestEndpoint();
        failingEndpoint.failWhen(message -> true);
        RetryPolicy policy = new RetryPolicy(2, 10, 2.0);

        MessageDispatcher firstDispatcher = new MessageDispatcher(first, failingEndpoint, policy, 5);
        first.inTransaction(ctx -> ctx.send("key-a", "doomed"));
        firstDispatcher.start();
        Await.until("message dead-lettered before crash",
                () -> first.messages().get(0).getStatus() == MessageStatus.DEAD_LETTER);
        firstDispatcher.close();
        assertThat(failingEndpoint.totalCalls()).isEqualTo(2);

        LocalDatabase second = LocalDatabase.fileBacked(directory);
        MessageEnvelope restored = second.messages().get(0);
        assertThat(restored.getStatus()).isEqualTo(MessageStatus.DEAD_LETTER);
        assertThat(restored.getAttempts()).isEqualTo(2);
        assertThat(restored.getDeadLetterReason()).contains("simulated downstream failure");
        assertThat(restored.getHistory()).hasSize(2);

        TestEndpoint healthyEndpoint = new TestEndpoint();
        MessageDispatcher secondDispatcher = new MessageDispatcher(second, healthyEndpoint, policy, 5);
        secondDispatcher.start();
        Thread.sleep(150);
        secondDispatcher.close();

        assertThat(healthyEndpoint.deliveries()).isEmpty();
        assertThat(second.messages().get(0).getStatus()).isEqualTo(MessageStatus.DEAD_LETTER);
        assertThat(second.messages().get(0).getAttempts()).isEqualTo(2);
    }

    @Test
    void emptyDirectoryStartsACleanDatabase() {
        LocalDatabase database = LocalDatabase.fileBacked(directory.resolve("fresh"));
        assertThat(database.messages()).isEmpty();
        assertThat(database.businessData()).isEmpty();
    }
}
