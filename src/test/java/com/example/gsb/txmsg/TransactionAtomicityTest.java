package com.example.gsb.txmsg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Requirement 1: business data and outgoing messages commit or roll back together. */
class TransactionAtomicityTest {

    private final LocalDatabase database = LocalDatabase.inMemory();

    @Test
    void commitWritesBusinessDataAndMessageTogether() {
        database.inTransaction(ctx -> {
            ctx.putBusinessData("order-1", "CREATED");
            ctx.send("order-1", "order-created");
        });

        assertThat(database.businessData()).containsEntry("order-1", "CREATED");
        assertThat(database.messages()).hasSize(1);

        MessageEnvelope message = database.messages().get(0);
        assertThat(message.getBusinessKey()).isEqualTo("order-1");
        assertThat(message.getPayload()).isEqualTo("order-created");
        assertThat(message.getStatus()).isEqualTo(MessageStatus.PENDING);
        assertThat(message.getSequence()).isEqualTo(1);
    }

    @Test
    void failureBeforeSendRollsBackBusinessData() {
        assertThatThrownBy(() -> database.inTransaction(ctx -> {
            ctx.putBusinessData("order-1", "CREATED");
            throw new IllegalStateException("database failure");
        })).isInstanceOf(IllegalStateException.class).hasMessage("database failure");

        assertThat(database.businessData()).isEmpty();
        assertThat(database.messages()).isEmpty();
    }

    @Test
    void failureAfterSendRollsBackEverything() {
        assertThatThrownBy(() -> database.inTransaction(ctx -> {
            ctx.putBusinessData("order-1", "CREATED");
            ctx.send("order-1", "order-created");
            throw new IllegalStateException("boom");
        })).hasMessage("boom");

        assertThat(database.businessData()).isEmpty();
        assertThat(database.messages()).isEmpty();
    }

    @Test
    void multipleOperationsCommitAsOneUnitInStagingOrder() {
        database.inTransaction(ctx -> {
            ctx.putBusinessData("a", "1");
            ctx.putBusinessData("b", "2");
            ctx.send("key-a", "m1");
            ctx.send("key-a", "m2");
        });

        assertThat(database.businessData())
                .hasSize(2)
                .containsEntry("a", "1")
                .containsEntry("b", "2");
        assertThat(database.messages()).hasSize(2);
        assertThat(database.messages())
                .extracting(MessageEnvelope::getPayload)
                .containsExactly("m1", "m2");
        assertThat(database.messages())
                .extracting(MessageEnvelope::getSequence)
                .containsExactly(1L, 2L);
    }

    @Test
    void rolledBackTransactionDoesNotPoisonLaterTransactions() {
        assertThatThrownBy(() -> database.inTransaction(ctx -> {
            ctx.send("key-a", "rolled-back");
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        database.inTransaction(ctx -> {
            ctx.putBusinessData("a", "kept");
            ctx.send("key-a", "kept-message");
        });

        assertThat(database.businessData()).containsEntry("a", "kept");
        assertThat(database.messages()).hasSize(1);
        assertThat(database.messages().get(0).getPayload()).isEqualTo("kept-message");
    }
}
