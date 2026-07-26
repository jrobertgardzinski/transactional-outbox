package com.jrobertgardzinski.outbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What an event must carry before the library will promise to deliver it — and the one field that is
 * allowed to be missing.
 */
class OutboxEventTest {

    @Test
    @DisplayName("a blank id, topic, type, key or payload is refused at construction, not at INSERT time")
    void the_essentials_are_required() {
        // Refused HERE rather than by a not-null column: the INSERT happens inside the caller's
        // business transaction, so a constraint violation there takes a legitimate operation down
        // with it. A malformed event should never reach the transaction.
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new OutboxEvent(
                " ", "t", "TYPE", "k", null, "{}")).getMessage().contains("id"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new OutboxEvent(
                "i", "", "TYPE", "k", null, "{}")).getMessage().contains("topic"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new OutboxEvent(
                "i", "t", null, "k", null, "{}")).getMessage().contains("type"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new OutboxEvent(
                "i", "t", "TYPE", null, null, "{}")).getMessage().contains("key"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new OutboxEvent(
                "i", "t", "TYPE", "k", null, "")).getMessage().contains("payload"));
    }

    @Test
    @DisplayName("a missing correlation id is fine — not every announcement happens inside a traced request")
    void the_correlation_id_stays_optional() {
        assertDoesNotThrow(() -> new OutboxEvent("i", "t", "TYPE", "k", null, "{}"));
    }

    @Test
    @DisplayName("newId fits the documented varchar(36) column and never repeats")
    void ids_are_fresh_and_fit_the_column() {
        String id = OutboxEvent.newId();

        assertEquals(36, id.length(), "the documented column is varchar(36)");
        assertDoesNotThrow(() -> UUID.fromString(id));
        assertNotEquals(id, OutboxEvent.newId(),
                "two deletions of the same aggregate are two obligations, not one");
    }
}
