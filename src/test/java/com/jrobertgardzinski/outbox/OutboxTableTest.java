package com.jrobertgardzinski.outbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one configuration surface the library has — the table name — and the shape it documents.
 *
 * <p>The name is interpolated into SQL (no JDBC driver binds an identifier as a parameter), so the
 * whitelist is not a nicety: it is the reason a service cannot turn a configuration value into a
 * statement. Everything else about the table is fixed, which is the other half of the same argument —
 * a knob that never needs turning is a knob that can be turned wrong.
 */
class OutboxTableTest {

    @Test
    @DisplayName("a plain lower-case identifier is accepted")
    void a_plain_name_is_accepted() {
        assertEquals("meme_events_outbox", OutboxTable.named("meme_events_outbox").name());
        assertEquals("_outbox2", OutboxTable.named("_outbox2").name());
    }

    @Test
    @DisplayName("anything that is not a plain identifier is refused — the name reaches SQL unescaped")
    void a_name_that_is_not_an_identifier_is_refused() {
        for (String hostile : new String[]{
                "outbox; drop table users",
                "outbox\" or \"1\"=\"1",
                "public.outbox",          // a qualified name would defeat the whitelist's point
                "Outbox",                 // upper case: unquoted identifiers fold, so it would lie
                "1outbox",
                "outbox-events",
                "outbox events",
                "",
                null}) {
            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                    () -> OutboxTable.named(hostile), "must refuse: " + hostile);
            assertTrue(refused.getMessage().contains("interpolated into SQL"),
                    "the message must say WHY the rule is strict: " + refused.getMessage());
        }
    }

    @Test
    @DisplayName("an identifier longer than PostgreSQL's 63 characters is refused")
    void an_over_long_name_is_refused() {
        assertEquals(63, OutboxTable.named("a".repeat(63)).name().length());
        assertThrows(IllegalArgumentException.class, () -> OutboxTable.named("a".repeat(64)));
    }

    @Test
    @DisplayName("the documented DDL names the service's own table everywhere, table and index alike")
    void the_ddl_is_rendered_for_the_named_table() {
        String ddl = OutboxTable.named("comment_events_outbox").ddl();

        assertTrue(ddl.contains("create table comment_events_outbox ("));
        assertTrue(ddl.contains("create index idx_comment_events_outbox_pending"
                        + " on comment_events_outbox (published_at, created_at)"),
                "two outboxes may share a schema, so the index name must be per-table too:\n" + ddl);
        assertTrue(ddl.contains("payload         text"),
                "text, not varchar(n): a richer event type must not fail the INSERT inside the"
                        + " business transaction:\n" + ddl);
        assertTrue(ddl.contains("published_at    timestamp"),
                "a timestamp, not a boolean: retention measures its forensic window from the"
                        + " DELIVERY, and from created_at it deleted the trail of anything that had"
                        + " been stuck — exactly the trail worth keeping:\n" + ddl);
        assertTrue(ddl.contains("attempts        int") && ddl.contains("next_attempt_at timestamp"),
                "without a backoff an undeliverable row sits at the head of an oldest-first batch"
                        + " for ever, and newer events are never selected again:\n" + ddl);
    }

    // That the DDL is not merely well-formed but actually CREATES a table the library's queries
    // work against is proven everywhere else in this suite: OutboxTestDatabase builds every fixture
    // by executing exactly this string.
}
