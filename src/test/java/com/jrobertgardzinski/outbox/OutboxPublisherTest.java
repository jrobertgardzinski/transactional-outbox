package com.jrobertgardzinski.outbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Properties (b) and (c): a row is marked published <strong>only</strong> after the broker confirmed,
 * and the first attempt does not make the announcing thread wait for that confirmation.
 *
 * <p>(c) is the property that is easy to lose in a refactor and impossible to notice in production
 * until a broker outage turns into a consumer-group rebalance, so it is pinned by a timing assertion
 * against a broker that never answers — not by inspecting the code's shape.
 */
class OutboxPublisherTest {

    private OutboxTestDatabase database;
    private TransactionalOutbox outbox;
    private FakeDispatch broker;
    private OutboxPublisher publisher;

    @BeforeEach
    void freshOutbox() {
        database = new OutboxTestDatabase("comment_events_outbox");
        outbox = new TransactionalOutbox(database.table(), database.connections(),
                SteerableClock.at("2026-07-26T10:00:00Z"));
        broker = new FakeDispatch(FakeDispatch.Behaviour.CONFIRM);
        publisher = new OutboxPublisher(outbox, broker);
    }

    private OutboxEvent committed(String id) throws SQLException {
        OutboxEvent event = new OutboxEvent(id, "comments-events", "COMMENTS_DELETED", "meme-1",
                "cid-1", "{\"id\":\"" + id + "\",\"type\":\"COMMENTS_DELETED\"}");
        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, event);
            tx.commit();
        }
        return event;
    }

    @Test
    @DisplayName("(b) a CONFIRMED first attempt marks the row — but on the DRAIN, never in the callback")
    void a_confirmed_send_marks_the_row() throws SQLException {
        publisher.publishWithoutWaiting(committed("e-1"));

        assertEquals(1, broker.sends());
        // The mark used to happen right here, inside the confirmation callback — which is the Kafka
        // producer's single I/O thread, and the mark is JDBC. A connection pool with nothing free
        // blocks for Hikari's default thirty seconds, and that froze ALL of the service's Kafka
        // sends: unrelated events hit their own delivery timeout and became more unpublished rows
        // needing more marks. This assertion is the one that keeps the mark off that thread.
        assertFalse(database.published("e-1"), "the producer's I/O thread must not touch the database");

        assertEquals(1, publisher.drainConfirmations());

        assertTrue(database.published("e-1"), "the republisher's pass writes the mark instead");
    }

    @Test
    @DisplayName("(b) a REJECTED send does NOT mark the row — delivery has not happened")
    void a_rejected_send_leaves_the_row_owed() throws SQLException {
        broker.behave(FakeDispatch.Behaviour.REJECT);

        publisher.publishWithoutWaiting(committed("e-2"));

        assertEquals(1, broker.sends());
        assertFalse(database.published("e-2"),
                "an unconfirmed send must not mark the row, or the republisher would forget it");
    }

    @Test
    @DisplayName("(b) a producer that refuses the record by THROWING is a failure, not an exception the caller sees")
    void a_throwing_producer_is_swallowed_and_the_row_stays() throws SQLException {
        broker.behave(FakeDispatch.Behaviour.THROW);

        publisher.publishWithoutWaiting(committed("e-3"));   // must not throw: the commit already happened

        assertFalse(database.published("e-3"));
    }

    @Test
    @DisplayName("(c) a broker that never answers does not hold the announcing thread — the mark rides the acknowledgement")
    void the_first_attempt_never_waits_for_the_broker() throws SQLException {
        // The hazard this replaced: a blocking first attempt on the announcing thread. With the
        // broker down, a cascade of N announcements from a broker LISTENER thread held it
        // N × patience — long enough to overrun max.poll.interval.ms and trigger a rebalance,
        // promoting a delivery problem to an availability problem.
        broker.behave(FakeDispatch.Behaviour.SILENT);
        OutboxEvent event = committed("e-4");

        long before = System.nanoTime();
        publisher.publishWithoutWaiting(event);
        long heldMillis = Duration.ofNanos(System.nanoTime() - before).toMillis();

        assertTrue(heldMillis < 2_000, "the announcing thread must not wait for the broker — took "
                + heldMillis + " ms");
        assertFalse(database.published("e-4"), "no acknowledgement yet — nothing to mark");

        broker.settleAllConfirmed();   // the acknowledgement finally arrives, on the producer's thread

        assertFalse(database.published("e-4"),
                "the callback records the confirmation but does NOT write it — see the drain below");

        publisher.drainConfirmations();

        assertTrue(database.published("e-4"), "and the drain, on a thread allowed to block, marks it");
    }

    @Test
    @DisplayName("(d) publishAndWait waits for the acknowledgement, then marks — the republisher's leg is delivered-first")
    void the_waiting_attempt_marks_after_the_acknowledgement() throws SQLException {
        assertEquals(OutboxPublisher.Attempt.CONFIRMED,
                publisher.publishAndWait(committed("e-5"), Duration.ofSeconds(5)));

        assertTrue(database.published("e-5"));
    }

    @Test
    @DisplayName("(d) publishAndWait gives up after its patience and leaves the row owed — no mark on a silence")
    void the_waiting_attempt_times_out_without_marking() throws SQLException {
        broker.behave(FakeDispatch.Behaviour.SILENT);

        // TIMED_OUT, not merely "not confirmed": a silence is about the BROKER, and the caller must
        // be able to tell it from a refusal, or an outage counts every row towards being given up on
        assertEquals(OutboxPublisher.Attempt.TIMED_OUT,
                publisher.publishAndWait(committed("e-6"), Duration.ofMillis(50)),
                "a silence is 'still owed', reported as such rather than thrown");

        assertFalse(database.published("e-6"));
    }

    @Test
    @DisplayName("(d) a rejected re-send reports failure and leaves the row for the next pass")
    void the_waiting_attempt_reports_a_rejection() throws SQLException {
        broker.behave(FakeDispatch.Behaviour.REJECT);

        // REJECTED: the broker ANSWERED, and its answer was no — that is about this event
        assertEquals(OutboxPublisher.Attempt.REJECTED,
                publisher.publishAndWait(committed("e-7"), Duration.ofSeconds(5)));

        assertFalse(database.published("e-7"));
    }

    @Test
    @DisplayName("(d) a producer that THROWS on the re-send is a timeout, not a rejection — Kafka throws synchronously during an outage")
    void the_waiting_attempt_treats_a_synchronous_throw_as_a_timeout() throws SQLException {
        // KafkaProducer.send() blocks on the metadata fetch for up to max.block.ms and then throws
        // TimeoutException SYNCHRONOUSLY — that is what a broker outage looks like from this thread.
        // Counting it as REJECTED counted every outage pass towards poisonAfter, and 25 rounds is
        // about eighteen hours: a weekend outage permanently removed the row from the poll.
        broker.behave(FakeDispatch.Behaviour.THROW);

        assertEquals(OutboxPublisher.Attempt.TIMED_OUT,
                publisher.publishAndWait(committed("e-8"), Duration.ofSeconds(5)),
                "a synchronous throw says nothing about the event — it must not count against it");

        assertFalse(database.published("e-8"));
    }

    @Test
    @DisplayName("(b)+(d) an acknowledgement arriving AFTER the patience is accepted through the drain — not discarded")
    void a_late_acknowledgement_still_marks_the_row() throws SQLException {
        // A slow broker is not a dead one. Discarding an ack that arrives after the patience left
        // the row owed, so every subsequent pass re-sent an already-delivered event — one duplicate
        // per pass for as long as the ack latency exceeded the patience.
        broker.behave(FakeDispatch.Behaviour.SILENT);
        OutboxEvent event = committed("e-9");

        assertEquals(OutboxPublisher.Attempt.TIMED_OUT,
                publisher.publishAndWait(event, Duration.ofMillis(50)));
        assertFalse(database.published("e-9"), "no acknowledgement yet — nothing to mark");

        broker.settleAllConfirmed();   // the acknowledgement arrives, late, on the producer's thread

        assertFalse(database.published("e-9"),
                "the late acknowledgement queues the mark but must NOT write it on that thread");

        assertEquals(1, publisher.drainConfirmations(),
                "the drain must find the late confirmation");
        assertTrue(database.published("e-9"),
                "a delivered event must be marked, however late the broker's word came");
    }
}
