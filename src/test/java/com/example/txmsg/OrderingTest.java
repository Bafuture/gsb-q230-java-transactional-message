package com.example.txmsg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

/** Messages sharing a business key are delivered strictly in production order. */
class OrderingTest {

    @Test
    void sameBusinessKeyDeliveredInSequenceOrder() {
        FakeClock clock = new FakeClock(0);
        TransactionalDatabase db = new TransactionalDatabase(new BusinessStore(), new MessageStore(), clock);
        CopyOnWriteArrayList<String> order = new CopyOnWriteArrayList<>();
        DeduplicatingConsumer consumer = new DeduplicatingConsumer(m -> order.add(m.payload()));
        MessageRelay relay = new MessageRelay(db.messageStore(), consumer, clock, 0, 10, 5);

        db.inTransaction(tx -> {
            tx.enqueue("k", "t", "e1");
            tx.enqueue("k", "t", "e2");
            tx.enqueue("k", "t", "e3");
        });

        relay.runOnce();
        assertThat(order).containsExactly("e1", "e2", "e3");
    }

    @Test
    void failingHeadBlocksLaterSameKeyButNotOtherKeys() {
        FakeClock clock = new FakeClock(0);
        TransactionalDatabase db = new TransactionalDatabase(new BusinessStore(), new MessageStore(), clock);
        CopyOnWriteArrayList<String> order = new CopyOnWriteArrayList<>();

        // "k-a#1" fails twice, then succeeds. "k-a#2" must wait for it.
        // "k-b#1" has another key and is free to go ahead.
        AtomicInteger headFailuresLeft = new AtomicInteger(2);
        Predicate<String> isFailingHead = "e-a1"::equals;
        MessageConsumer consumer = m -> {
            if (isFailingHead.test(m.payload()) && headFailuresLeft.get() > 0) {
                headFailuresLeft.decrementAndGet();
                throw new RuntimeException("head transient failure");
            }
            order.add(m.payload());
        };
        MessageRelay relay = new MessageRelay(db.messageStore(), consumer, clock, 0, 100, 5);

        final String[] a1 = new String[1];
        db.inTransaction(tx -> {
            a1[0] = tx.enqueue("k-a", "t", "e-a1").id();
            tx.enqueue("k-a", "t", "e-a2");
            tx.enqueue("k-b", "t", "e-b1");
        });

        relay.runOnce();
        // k-b delivered; k-a#2 blocked behind retrying k-a#1.
        assertThat(order).containsExactly("e-b1");
        assertThat(db.messageStore().find(a1[0]).attempts()).isEqualTo(1);

        clock.advanceMillis(100);
        relay.runOnce(); // k-a#1 fails again; #2 still blocked
        assertThat(order).containsExactly("e-b1");

        clock.advanceMillis(200);
        relay.runOnce(); // k-a#1 succeeds; #2 is unblocked and delivered in the same pass.
        assertThat(order).containsExactly("e-b1", "e-a1", "e-a2");

        // Production order is preserved for k-a even though k-b overtook it.
        assertThat(List.copyOf(order).indexOf("e-a1")).isLessThan(List.copyOf(order).indexOf("e-a2"));
    }
}
