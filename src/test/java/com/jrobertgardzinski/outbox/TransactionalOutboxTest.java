package com.jrobertgardzinski.outbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property (a) and the table's own mechanics: the row shares the fate of the transaction that wrote
 * it, the mark is a separate act, and retention forgets only what has demonstrably been delivered.
 *
 * <p>No framework anywhere in this file — a JDBC connection with autocommit off IS the transaction.
 * That is the claim {@link TransactionalOutbox#append} makes, so it is the claim the test makes.
 */
class TransactionalOutboxTest {

    private OutboxTestDatabase database;
    private SteerableClock clock;
    private TransactionalOutbox outbox;

    @BeforeEach
    void freshOutbox() {
        database = new OutboxTestDatabase("meme_events_outbox");
        clock = SteerableClock.at("2026-07-26T10:00:00Z");
        outbox = new TransactionalOutbox(database.table(), database.connections(), clock);
    }

    private static OutboxEvent event(String id, String key) {
        return new OutboxEvent(id, "memes-events", "MEME_DELETED", key, "cid-1",
                "{\"type\":\"MEME_DELETED\",\"memeId\":\"" + key + "\",\"eventId\":\"" + id + "\"}");
    }

    @Test
    @DisplayName("(a) a rolled-back transaction leaves NO row — the announcement cannot outlive the change")
    void rollback_leaves_no_row() throws SQLException {
        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, event("e-1", "still-alive"));
            assertEquals(0, database.rows(), "uncommitted, so invisible to another connection");
            tx.rollback();
        }

        assertEquals(0, database.rows(), "the row must share the fate of the transaction that wrote it");
    }

    @Test
    @DisplayName("(a) a committed transaction leaves exactly one unpublished row — an outstanding obligation")
    void commit_leaves_an_unpublished_row() throws SQLException {
        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, event("e-2", "gone"));
            tx.commit();
        }

        assertEquals(1, database.rows());
        assertFalse(database.published("e-2"),
                "a written row is not a delivered event — only a confirmation may mark it");
    }

    @Test
    @DisplayName("(a) outside a transaction the row commits on its own — nothing to be atomic with")
    void without_a_transaction_the_row_stands_alone() throws SQLException {
        try (Connection autocommit = database.connections().get()) {
            outbox.append(autocommit, event("e-3", "orphan"));
        }

        assertEquals(1, database.rows());
    }

    @Test
    @DisplayName("(a) a failed append throws unchecked, so the caller's transaction rolls back instead of committing un-announced")
    void a_failed_append_refuses_to_be_ignored() throws SQLException {
        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, event("duplicate", "first"));
            tx.commit();
        }

        try (Connection tx = database.openTransaction()) {
            OutboxException refused = assertThrows(OutboxException.class,
                    () -> outbox.append(tx, event("duplicate", "second")));
            assertTrue(refused.getMessage().contains("must not commit"),
                    "the message must say what the caller is expected to do: " + refused.getMessage());
            tx.rollback();
        }
    }

    @Test
    @DisplayName("append neither commits nor closes the caller's connection — the transaction stays the caller's")
    void append_does_not_touch_the_transaction_it_joined() throws SQLException {
        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, event("e-4", "mine"));

            assertFalse(tx.getAutoCommit(), "autocommit must be left exactly as the caller set it");
            assertFalse(tx.isClosed(), "the library must not close a connection it did not open");
            assertEquals(0, database.rows(), "and must not have committed on the caller's behalf");
            tx.rollback();
        }
    }

    @Test
    @DisplayName("(g) a payload far wider than the canary threshold is stored byte-for-byte — TEXT, no silent cliff")
    void a_fat_payload_survives_intact() throws SQLException {
        String fat = "{\"type\":\"SOME_RICHER_EVENT\",\"blob\":\"" + "x".repeat(4096) + "\"}";

        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, new OutboxEvent("fat", "memes-events", "SOME_RICHER_EVENT", "fat",
                    null, fat));
            tx.commit();
        }

        assertEquals(fat, database.payload("fat"),
                "a future event type must not be cut short — the canary warns, the column absorbs");
    }

    @Test
    @DisplayName("(g) the correlation id lives IN the row, so a republication hours later still carries it")
    void the_correlation_id_is_stored_not_ambient() throws SQLException {
        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, new OutboxEvent("traced", "memes-events", "MEME_DELETED", "traced",
                    "cid-of-the-delete", "{}"));
            tx.commit();
        }
        assertEquals("cid-of-the-delete", database.cid("traced"));

        clock.advance(Duration.ofHours(3));
        OutboxEvent rebuilt = outbox.pendingOlderThan(Duration.ofSeconds(30), 10).getFirst();

        assertEquals("cid-of-the-delete", rebuilt.cid(),
                "the re-sent event must carry the trace of the request that announced it, not the"
                        + " (empty) context of the scheduler thread re-sending it");
    }

    @Test
    @DisplayName("(b) markPublished flips exactly one row, on its own connection — safe from a producer callback")
    void mark_published_flips_one_row() throws SQLException {
        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, event("e-5", "one"));
            outbox.append(tx, event("e-6", "two"));
            tx.commit();
        }

        outbox.markPublished("e-5");

        assertTrue(database.published("e-5"));
        assertFalse(database.published("e-6"));
    }

    @Test
    @DisplayName("(d) pendingOlderThan returns only unpublished rows past the age, oldest first")
    void pending_is_unpublished_and_old_enough() throws SQLException {
        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, event("old-owed", "old-owed"));
            outbox.append(tx, event("old-done", "old-done"));
            outbox.append(tx, event("fresh-owed", "fresh-owed"));
            tx.commit();
        }
        outbox.markPublished("old-done");
        database.backdateBySeconds("old-owed", 60);
        database.backdateBySeconds("old-done", 60);

        List<OutboxEvent> pending = outbox.pendingOlderThan(Duration.ofSeconds(30), 10);

        assertEquals(List.of("old-owed"), pending.stream().map(OutboxEvent::id).toList(),
                "a published row is done; a fresh one may still have an attempt in flight");
    }

    @Test
    @DisplayName("(d) the re-send poll is capped, so the first pass after a long outage is bounded")
    void pending_respects_its_limit() throws SQLException {
        try (Connection tx = database.openTransaction()) {
            for (int i = 0; i < 25; i++) {
                outbox.append(tx, event("backlog-" + i, "backlog"));
            }
            tx.commit();
        }
        database.backdateBySeconds("backlog", 60);

        assertEquals(10, outbox.pendingOlderThan(Duration.ofSeconds(30), 10).size());
        assertEquals(25, outbox.pendingOlderThan(Duration.ofSeconds(30), 500).size(),
                "the cap is a cap, not a page — a smaller backlog comes back whole");
    }

    @Test
    @DisplayName("(e) retention reaps DELIVERED rows past the window and nothing else")
    void retention_reaps_only_delivered_rows_past_the_window() throws SQLException {
        try (Connection tx = database.openTransaction()) {
            outbox.append(tx, event("old-done", "delivered-long-ago"));
            outbox.append(tx, event("new-done", "delivered-just-now"));
            outbox.append(tx, event("old-owed", "still-owed"));
            tx.commit();
        }
        outbox.markPublished("old-done");
        outbox.markPublished("new-done");
        database.backdateBySeconds("delivered-long-ago", Duration.ofHours(25).toSeconds());
        database.backdateBySeconds("still-owed", Duration.ofHours(25).toSeconds());

        int reaped = outbox.deletePublishedOlderThan(Duration.ofHours(24), 500, 4);

        assertEquals(1, reaped);
        assertEquals(0, database.count("id = ?", "old-done"),
                "a delivered event past retention has no value left — the row must go");
        assertEquals(1, database.count("id = ?", "new-done"),
                "retention must not touch a delivered row younger than the window");
        assertEquals(1, database.count("id = ?", "old-owed"),
                "an UNDELIVERED row is never reaped, however old — it still carries an obligation");
    }

    @Test
    @DisplayName("(e) retention is batched and capped: a big backlog goes N×batch per pass, the rest next pass")
    void retention_deletes_in_capped_batches() throws SQLException {
        // The shape of the FIRST pass after this feature ships on a service that has been deleting
        // for months. One statement over all of it would be a single long transaction on the
        // scheduler thread, blocking the re-send leg behind it.
        try (Connection tx = database.openTransaction()) {
            for (int i = 0; i < 1200; i++) {
                outbox.append(tx, event("bulk-" + i, "bulk"));
            }
            tx.commit();
        }
        for (int i = 0; i < 1200; i++) {
            outbox.markPublished("bulk-" + i);
        }
        database.backdateBySeconds("bulk", Duration.ofHours(25).toSeconds());

        int firstPass = outbox.deletePublishedOlderThan(Duration.ofHours(24), 500, 2);

        assertEquals(1000, firstPass, "a capped pass deletes exactly what the cap promises — no more"
                + " (the point) and no less (or a backlog would never drain)");
        assertEquals(200, database.rows(), "the remainder waits for the next pass");

        assertEquals(200, outbox.deletePublishedOlderThan(Duration.ofHours(24), 500, 2),
                "the next pass takes what is left and stops early");
        assertEquals(0, database.rows());
        assertEquals(0, outbox.deletePublishedOlderThan(Duration.ofHours(24), 500, 2),
                "an empty backlog costs one short statement, not a batch loop");
    }

    @Test
    @DisplayName("(f) a non-positive batch limit is refused — a loop that deletes nothing reads like an empty table")
    void retention_refuses_non_positive_limits() {
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> outbox.deletePublishedOlderThan(Duration.ofHours(24), 0, 4))
                .getMessage().contains("batch rows"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> outbox.deletePublishedOlderThan(Duration.ofHours(24), 500, -1))
                .getMessage().contains("batches per pass"));
    }
}
