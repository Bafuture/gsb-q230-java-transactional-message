package com.example.txmsg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Business data and outbox message commit atomically, or roll back together. */
class TransactionalAtomicityTest {

    private TransactionalDatabase db;

    @BeforeEach
    void setUp() {
        db = new TransactionalDatabase();
    }

    @Test
    void commitWritesBusinessDataAndMessageAtomically() {
        db.inTransaction(tx -> {
            tx.putBusiness("order-1", "CREATED");
            tx.enqueue("order-1", "OrderCreated", "{\"orderId\":\"order-1\"}");
        });

        assertThat(db.businessStore().find("order-1").value()).isEqualTo("CREATED");
        assertThat(db.messageStore().snapshot()).hasSize(1);
        OutboxMessage message = db.messageStore().snapshot().get(0);
        assertThat(message.status()).isEqualTo(MessageStatus.PENDING);
        assertThat(message.businessKey()).isEqualTo("order-1");
    }

    @Test
    void rollbackDiscardsBusinessDataAndMessageTogether() {
        assertThatThrownBy(() -> db.inTransaction(tx -> {
            tx.putBusiness("order-2", "CREATED");
            tx.enqueue("order-2", "OrderCreated", "{}");
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(db.businessStore().find("order-2")).isNull();
        assertThat(db.messageStore().snapshot()).isEmpty();
    }

    @Test
    void uncommittedWritesAreNotVisibleOutsideTheTransaction() {
        assertThatThrownBy(() -> db.inTransaction(tx -> {
            tx.putBusiness("order-3", "CREATED");
            tx.enqueue("order-3", "OrderCreated", "{}");
            // read-your-own-writes inside the transaction
            assertThat(tx.getBusiness("order-3").value()).isEqualTo("CREATED");
            // but the store is not yet committed
            assertThat(db.messageStore().snapshot()).isEmpty();
            throw new IllegalStateException("abort");
        }));

        assertThat(db.businessStore().find("order-3")).isNull();
    }

    @Test
    void multipleMessagesInOneTransactionCommitInProductionOrder() {
        db.inTransaction(tx -> {
            tx.enqueue("k", "E1", "1");
            tx.enqueue("k", "E2", "2");
            tx.enqueue("k", "E3", "3");
        });

        assertThat(db.messageStore().snapshot())
                .extracting(OutboxMessage::payload)
                .containsExactly("1", "2", "3");
    }
}
