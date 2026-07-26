package com.jrobertgardzinski.outbox;

import java.util.regex.Pattern;

/**
 * Which table this outbox lives in — and the DDL that table must have.
 *
 * <p><strong>Why the name is a parameter and the migration is not shipped.</strong> Every service
 * in the estate owns its own schema history (Flyway, its own numbering, its own baseline). A
 * library that shipped a migration would have to claim a version number in someone else's
 * sequence, would collide the first time two services sat on one database, and would take the
 * schema out of the hands of the team that operates it. So the library ships the SHAPE and the
 * service ships the migration: copy {@link #ddl()} into your own {@code V<n>__…sql}. The library's
 * own tests create the table from exactly that string, so the documented shape cannot drift away
 * from the SQL the library actually runs.
 *
 * <p>Two services may name their tables differently ({@code meme_events_outbox},
 * {@code comment_events_outbox}) and two outboxes may even live in one schema — the name is the
 * only variable. Column names are FIXED: they are the library's queries, not a configuration
 * surface, and every knob that never needs turning is a knob that can be turned wrong.
 */
public final class OutboxTable {

    /**
     * The table name is interpolated into SQL, because an identifier cannot be a bind parameter in
     * any JDBC driver. So it is whitelisted rather than escaped: a plain unquoted SQL identifier,
     * nothing else gets near a statement. 63 characters is PostgreSQL's own identifier limit.
     */
    private static final Pattern PLAIN_IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]{0,62}");

    private final String name;

    private OutboxTable(String name) {
        this.name = name;
    }

    /**
     * @param name the table name, lower case — the estate's databases are PostgreSQL (and H2 in
     *             PostgreSQL mode with {@code DATABASE_TO_LOWER=TRUE}), where an unquoted
     *             identifier folds to lower case anyway; requiring it up front means the name in
     *             the migration, in the code and in the error message all read the same
     * @throws IllegalArgumentException if the name is not a plain SQL identifier
     */
    public static OutboxTable named(String name) {
        if (name == null || !PLAIN_IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("outbox table name must be a plain lower-case SQL"
                    + " identifier matching " + PLAIN_IDENTIFIER.pattern()
                    + " (it is interpolated into SQL, so it is whitelisted, not escaped), was: " + name);
        }
        return new OutboxTable(name);
    }

    public String name() {
        return name;
    }

    /**
     * The required table shape, ready to paste into the consuming service's own Flyway migration.
     *
     * <p>Notes on the choices, because a migration outlives the reasoning that produced it:
     *
     * <ul>
     *   <li>{@code payload text}, not {@code varchar(n)}. The first implementation sized it at
     *       1024 chars for a one-line JSON; the first richer event type to cross that line would
     *       not be truncated quietly, it would fail the INSERT INSIDE THE BUSINESS TRANSACTION and
     *       take the operation down with it. In PostgreSQL {@code text} is the same varlena storage
     *       as {@code varchar}, so the cliff costs nothing to remove. The canary that used to be
     *       that column limit survives as a log line (see
     *       {@link TransactionalOutbox#EXPECTED_MAX_PAYLOAD_CHARS}).</li>
     *   <li>{@code published boolean}, not a {@code published_at} timestamp. The mark is a fact
     *       about delivery, and the only question ever asked of it is "is this row still owed?".
     *       A nullable timestamp would invite a second, drifting answer to that question.</li>
     *   <li>The index is on {@code (published, created_at)} because that is the republisher's only
     *       predicate, on both legs: "unpublished and older than" and "published and older than".
     *       Without it the poll is a table scan every few seconds, forever.</li>
     * </ul>
     */
    public String ddl() {
        return """
                -- Transactional outbox (com.jrobertgardzinski:transactional-outbox). Rows are
                -- fully built, ready-to-send events written in the SAME transaction as the change
                -- that announces them: a rollback takes the announcement with it, and a crash
                -- between the commit and the send loses nothing — the republisher re-sends
                -- whatever stayed unpublished. The row's id IS the event id inside the payload,
                -- so a redelivery is a recognisable duplicate rather than a second event.
                create table %1$s (
                    id         varchar(36) primary key,  -- also the payload's event id
                    topic      varchar(64) not null,
                    event_type varchar(64) not null,
                    event_key  varchar(64) not null,     -- partition key
                    cid        varchar(64),              -- correlation id, stamped at announce time
                    payload    text        not null,
                    created_at timestamp   not null,
                    published  boolean     not null default false
                );

                -- the republisher polls "unpublished and old enough" every few seconds, and reaps
                -- "published and old enough" in the same pass — keep both off a table scan
                create index idx_%1$s_pending on %1$s (published, created_at);
                """.formatted(name);
    }

    @Override
    public String toString() {
        return "OutboxTable[" + name + "]";
    }
}
