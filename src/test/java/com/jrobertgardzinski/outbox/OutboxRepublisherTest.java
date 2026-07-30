package com.jrobertgardzinski.outbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The pass that makes the library's promise true, end to end: a committed event whose send failed is
 * delivered later — the SAME event, byte for byte — and a delivered one is eventually forgotten.
 *
 * <p>This is the file that would fail if the extraction had eaten a property, so it asserts the
 * durability claim rather than the shape of the code: what the broker received, what the table looks
 * like afterwards, and in which order the two legs of a pass ran.
 */
class OutboxRepublisherTest {

    private static final Duration RETENTION = Duration.ofHours(24);

    private OutboxTestDatabase database;
    private SteerableClock clock;
    private TransactionalOutbox outbox;
    private FakeDispatch broker;
    private OutboxPublisher publisher;
    private OutboxRepublisher republisher;

    @BeforeEach
    void freshOutbox() {
        database = new OutboxTestDatabase("meme_events_outbox");
        clock = SteerableClock.at("2026-07-26T10:00:00Z");
        outbox = new TransactionalOutbox(database.table(), database.connections(), clock);
        broker = new FakeDispatch(FakeDispatch.Behaviour.CONFIRM);
        publisher = new OutboxPublisher(outbox, broker);
        republisher = new OutboxRepublisher(outbox, publisher,
                // patience shortened from the 5s default: these tests answer instantly or not at all,
                // and a timeout case must not cost the suite five seconds
                new RepublisherSettings(Duration.ofSeconds(30), Duration.ofMillis(200), RETENTION,
                        500, 4, 500, Duration.ofSeconds(30), Duration.ofHours(1), 25));
    }

    private OutboxEvent announce(String id, String key) throws SQLException {
        OutboxEvent event = new OutboxEvent(id, "memes-events", "MEME_DELETED", key, "cid-" + key,
                "{\"type\":\"MEME_DELETED\",\"memeId\":\"" + key + "\",\"eventId\":\"" + id + "\"}");
        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, event);
            tx.commit();
        }
        publisher.publishWithoutWaiting(event);
        // A confirmation no longer writes its own mark — it queues, and the republisher's pass
        // writes it, because the callback runs on the producer's I/O thread and the mark is JDBC.
        // Draining here is what a real pass does moments later; without it these tests would set up
        // a state production never reaches (confirmed, permanently unmarked).
        publisher.drainConfirmations();
        return event;
    }

    @Test
    @DisplayName("(d) a committed event whose send failed is re-sent later — the SAME event — and marked once confirmed")
    void the_republisher_delivers_what_the_first_attempt_could_not() throws SQLException {
        broker.behave(FakeDispatch.Behaviour.REJECT);
        OutboxEvent announced = announce("e-1", "orphan-thread");
        assertFalse(database.published("e-1"), "the failed send must leave the row owed");
        OutboxEvent firstAttempt = broker.lastHanded();

        // the broker comes back; the row comes of age; the republisher takes over
        broker.behave(FakeDispatch.Behaviour.CONFIRM);
        clock.advance(Duration.ofMinutes(1));

        OutboxRepublisher.Pass pass = republisher.runOnce();

        assertEquals(new OutboxRepublisher.Pass(0, 0, 1, 1, 0), pass);
        assertEquals(2, broker.sends());
        assertEquals(firstAttempt.payload(), broker.lastHanded().payload(),
                "the redelivery must be the SAME event — same payload, same event id inside it, so a"
                        + " consumer can recognise the duplicate");
        assertEquals(announced.cid(), broker.lastHanded().cid(),
                "…and the same correlation id, rebuilt from the row rather than from an empty"
                        + " scheduler-thread context");
        assertTrue(database.published("e-1"), "a confirmed redelivery must mark the row");

        assertEquals(new OutboxRepublisher.Pass(0, 0, 0, 0, 0), republisher.runOnce());
        assertEquals(2, broker.sends(), "delivered means done — no third send");
    }

    @Test
    @DisplayName("(d) a fresh unpublished row is left alone — its first attempt may still be in flight")
    void the_republisher_leaves_fresh_rows_to_the_first_attempt() throws SQLException {
        broker.behave(FakeDispatch.Behaviour.REJECT);
        announce("e-2", "just-now");

        republisher.runOnce();   // no clock advance: the row is seconds old

        assertEquals(1, broker.sends(), "re-sending now would race an attempt that may still land");
        assertFalse(database.published("e-2"));
    }

    @Test
    @DisplayName("(d) a happy-path event is published exactly once — the republisher does not double a marked row")
    void a_delivered_event_is_never_re_sent() throws SQLException {
        announce("e-3", "gone");
        assertTrue(database.published("e-3"));

        clock.advance(Duration.ofMinutes(1));   // old enough to qualify, but already delivered
        republisher.runOnce();

        assertEquals(1, broker.sends());
    }

    @Test
    @DisplayName("(d) a re-send the broker never answers costs one pass, not the event")
    void an_unanswered_re_send_leaves_the_row_for_the_next_pass() throws SQLException {
        broker.behave(FakeDispatch.Behaviour.REJECT);
        announce("e-4", "stranded");
        broker.behave(FakeDispatch.Behaviour.SILENT);
        clock.advance(Duration.ofMinutes(1));

        assertEquals(new OutboxRepublisher.Pass(0, 0, 1, 0, 0), republisher.runOnce(),
                "one attempt, nothing confirmed");
        assertFalse(database.published("e-4"));

        broker.behave(FakeDispatch.Behaviour.CONFIRM);
        // The failed attempt above bought the row a backoff, so the IMMEDIATE next pass leaves it
        // alone — that is the whole mechanism that stops an undeliverable event from occupying the
        // head of an oldest-first batch for ever. Prove it, then wait it out.
        assertEquals(new OutboxRepublisher.Pass(0, 0, 0, 0, 0), republisher.runOnce(),
                "a row that just failed is not eligible again until its backoff elapses");

        clock.advance(Duration.ofMinutes(1));   // past the 30s first backoff

        assertEquals(new OutboxRepublisher.Pass(0, 0, 1, 1, 0), republisher.runOnce());
        assertTrue(database.published("e-4"), "the next eligible pass delivers it");
    }

    @Test
    @DisplayName("(d) a broker outage never poisons a row, however long it lasts")
    void a_silent_broker_does_not_count_against_the_event() throws SQLException {
        // The distinction this asserts is the difference between an outage and data loss. A silent
        // broker used to count towards poisonAfter exactly like a rejection, and 25 rounds of
        // backoff is about eighteen hours — so a broker down over a weekend permanently removed the
        // oldest part of the backlog from the poll, breaking the library's one promise with no way
        // back except a hand-written UPDATE.
        broker.behave(FakeDispatch.Behaviour.REJECT);
        announce("outage-1", "stranded");
        broker.behave(FakeDispatch.Behaviour.SILENT);

        for (int pass = 0; pass < 40; pass++) {          // far past poisonAfter = 25
            clock.advance(Duration.ofMinutes(2));        // past any backoff this could have set
            republisher.runOnce();
        }

        broker.behave(FakeDispatch.Behaviour.CONFIRM);
        clock.advance(Duration.ofMinutes(2));
        OutboxRepublisher.Pass recovery = republisher.runOnce();

        assertEquals(1, recovery.resent(),
                "after forty silent passes the row must STILL be eligible — a broker that says"
                        + " nothing says nothing about the event");
        assertEquals(1, recovery.confirmed());
        assertTrue(database.published("outage-1"), "and it goes out the moment the broker returns");
    }

    @Test
    @DisplayName("(d) a producer that THROWS synchronously never poisons a row — Kafka throws like that during an outage")
    void a_throwing_producer_does_not_count_against_the_event() throws SQLException {
        // The other face of an outage: KafkaProducer.send() does not always go silent — with no
        // metadata in the cache (a pod restarted mid-outage) it throws TimeoutException
        // SYNCHRONOUSLY after max.block.ms. That throw used to count as REJECTED, so attempts
        // climbed on every pass and a weekend-long outage reached poisonAfter = 25, removing the
        // row from the poll for ever — the exact defect the SILENT test above guards against,
        // reachable through the synchronous path instead.
        broker.behave(FakeDispatch.Behaviour.REJECT);
        announce("outage-2", "stranded");
        broker.behave(FakeDispatch.Behaviour.THROW);

        for (int pass = 0; pass < 40; pass++) {          // far past poisonAfter = 25
            clock.advance(Duration.ofMinutes(2));        // past any backoff this could have set
            assertEquals(0, republisher.runOnce().poisoned(),
                    "a synchronous throw must never poison — it says nothing about the event");
        }

        broker.behave(FakeDispatch.Behaviour.CONFIRM);
        clock.advance(Duration.ofMinutes(2));
        OutboxRepublisher.Pass recovery = republisher.runOnce();

        assertEquals(1, recovery.resent(),
                "after forty throwing passes the row must STILL be eligible — attempts must not"
                        + " have grown towards the ceiling");
        assertEquals(1, recovery.confirmed());
        assertTrue(database.published("outage-2"), "and it goes out the moment the broker returns");
    }

    @Test
    @DisplayName("(d) an acknowledgement arriving after the patience stops the duplicates — the next pass marks instead of re-sending")
    void a_late_acknowledgement_prevents_the_re_send() throws SQLException {
        // A broker whose ack latency exceeds the patience used to cause a duplicate on EVERY pass:
        // the late ack was discarded, the row stayed owed, the next pass re-sent. The late ack must
        // ride the same queue the first attempt uses, so the next pass marks the row and leaves it be.
        broker.behave(FakeDispatch.Behaviour.REJECT);
        announce("slow-1", "slow-broker");
        broker.behave(FakeDispatch.Behaviour.SILENT);
        clock.advance(Duration.ofMinutes(1));

        assertEquals(new OutboxRepublisher.Pass(0, 0, 1, 0, 0), republisher.runOnce(),
                "the re-send goes out and the patience elapses unanswered");
        assertEquals(2, broker.sends());

        broker.settleAllConfirmed();   // the broker answers — late, after the patience
        clock.advance(Duration.ofMinutes(1));   // past the backoff the timed-out attempt bought

        assertEquals(new OutboxRepublisher.Pass(1, 0, 0, 0, 0), republisher.runOnce(),
                "the next pass must MARK the delivered row, not re-send it");
        assertTrue(database.published("slow-1"));
        assertEquals(2, broker.sends(), "delivered means done — no duplicate for the late ack");
    }

    @Test
    @DisplayName("(d) a broker that ANSWERS no still poisons the row — that part must not regress")
    void a_rejecting_broker_still_reaches_the_ceiling() throws SQLException {
        broker.behave(FakeDispatch.Behaviour.REJECT);
        announce("poison-1", "undeliverable");

        OutboxRepublisher.Pass poisoning = null;
        for (int pass = 0; pass < 30; pass++) {
            clock.advance(Duration.ofHours(2));          // past the 1 h backoff cap
            OutboxRepublisher.Pass result = republisher.runOnce();
            if (result.poisoned() > 0) {
                poisoning = result;
                break;
            }
        }

        assertTrue(poisoning != null, "a row the broker keeps REFUSING must eventually be given up on");
        clock.advance(Duration.ofHours(2));
        assertEquals(0, republisher.runOnce().resent(),
                "and once poisoned it stops occupying the head of an oldest-first batch");
    }

    @Test
    @DisplayName("(e) the pass reaps delivered rows past retention and does not touch the undelivered")
    void a_pass_reaps_delivered_rows_only() throws SQLException {
        announce("done-old", "delivered-long-ago");
        announce("done-new", "delivered-just-now");
        broker.behave(FakeDispatch.Behaviour.REJECT);
        announce("owed-old", "still-owed");
        database.backdateBySeconds("delivered-long-ago", Duration.ofHours(25).toSeconds());
        database.backdateBySeconds("still-owed", Duration.ofHours(25).toSeconds());

        OutboxRepublisher.Pass pass = republisher.runOnce();

        assertEquals(1, pass.reaped());
        assertEquals(1, pass.resent(), "the aged unpublished row is retried in the same pass");
        assertEquals(0, database.count("id = ?", "done-old"));
        assertEquals(1, database.count("id = ?", "done-new"),
                "retention must not touch a delivered row younger than the window");
        assertEquals(1, database.count("id = ?", "owed-old"),
                "an undelivered row is never reaped, however old");
    }

    @Test
    @DisplayName("(e) retention runs BEFORE the re-send, so a row this pass marks survives until the next one")
    void retention_runs_before_the_re_send() throws SQLException {
        // The order is a guarantee, not a preference: reaping after sending would put the reap and
        // the mark in the same breath, and a retention window misconfigured to near-zero would then
        // delete the row of an event whose confirmation is still in flight. Proven by looking at the
        // table from INSIDE the send: if retention already ran, the aged delivered row is gone.
        announce("done-old", "delivered-long-ago");
        broker.behave(FakeDispatch.Behaviour.REJECT);
        announce("owed-old", "still-owed");
        database.backdateBySeconds("delivered-long-ago", Duration.ofHours(25).toSeconds());
        database.backdateBySeconds("still-owed", Duration.ofHours(25).toSeconds());
        broker.behave(FakeDispatch.Behaviour.CONFIRM);
        AtomicInteger rowsWhenTheBrokerSawIt = new AtomicInteger(-1);
        broker.onSend(event -> rowsWhenTheBrokerSawIt.set(database.rows()));

        republisher.runOnce();

        assertEquals(1, rowsWhenTheBrokerSawIt.get(),
                "by the time the pass re-sends, retention has already reaped the aged delivered row"
                        + " — only the row being re-sent is left");
    }

    @Test
    @DisplayName("(d) the re-send leg is capped per pass, so a long outage drains over several passes")
    void the_re_send_leg_is_capped_per_pass() throws SQLException {
        OutboxRepublisher capped = new OutboxRepublisher(outbox, publisher,
                new RepublisherSettings(Duration.ofSeconds(30), Duration.ofMillis(200), RETENTION,
                        500, 4, /* resendBatchRows */ 3,
                        Duration.ofSeconds(30), Duration.ofHours(1), 25));
        broker.behave(FakeDispatch.Behaviour.REJECT);
        try (Connection tx = database.openTransaction()) {
            for (int i = 0; i < 8; i++) {
                outbox.append(tx, new OutboxEvent("backlog-" + i, "memes-events", "MEME_DELETED",
                        "backlog", null, "{}"));
            }
            tx.commit();
        }
        clock.advance(Duration.ofMinutes(1));
        broker.behave(FakeDispatch.Behaviour.CONFIRM);

        assertEquals(3, capped.runOnce().resent());
        assertEquals(3, capped.runOnce().resent());
        assertEquals(2, capped.runOnce().resent(), "the tail of the backlog");
        assertEquals(0, capped.runOnce().resent());
        assertEquals(0, database.count("published_at IS NULL"));
    }

    @Test
    @DisplayName("a pass never throws — a scheduler that cancels a failing task would retire the guarantee")
    void a_broken_pass_is_swallowed_so_the_schedule_survives() {
        // Spring's fixedDelay scheduler abandons a task whose invocation threw. A database hiccup
        // must therefore cost one pass, not the process's whole durability mechanism.
        database.run("DROP TABLE " + database.table().name());

        assertEquals(new OutboxRepublisher.Pass(0, 0, 0, 0, 0), republisher.runOnce());
    }
}
