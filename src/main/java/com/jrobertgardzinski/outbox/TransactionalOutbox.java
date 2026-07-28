package com.jrobertgardzinski.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * The outbox table: fully built, ready-to-send event rows that share the transaction of the change
 * announcing them. Plain JDBC, no framework — {@link #append} joins whatever transaction the caller
 * has open, the other three operations run on their own short connections.
 *
 * <p>The durability promise assembled from the four methods below:
 * <ol>
 *   <li>{@link #append} writes the event in the caller's transaction — a rollback means no row,
 *       which means no event: an announcement can never outlive the change it announces.</li>
 *   <li>{@link #markPublished} is called ONLY after the broker confirmed (delivered-first). Until
 *       then the row is an outstanding obligation.</li>
 *   <li>{@link #pendingOlderThan} finds the obligations nobody honoured — a crash between the commit
 *       and the send, a broker outage during it.</li>
 *   <li>{@link #deletePublishedOlderThan} forgets what has demonstrably been delivered, so the table
 *       does not grow by one row per event forever.</li>
 * </ol>
 * {@link OutboxPublisher} and {@link OutboxRepublisher} drive them in the right order; this class
 * only knows SQL.
 */
public final class TransactionalOutbox {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionalOutbox.class);

    /**
     * A payload wider than this gets a log line — not a rejection, not a truncation.
     *
     * <p>It is the width the first implementation's column was sized at ({@code varchar(1024)}, for
     * a one-line JSON). The documented DDL uses {@code text}, so nothing is cut short any more and
     * this number no longer constrains anything; what it still does is notice. An event type that
     * suddenly ships kilobytes is worth a look BEFORE it becomes routine — it is usually a payload
     * that grew a list (every comment id of a thread, every item of a collection), which is the
     * shape that later trips the broker's own {@code max.request.size} on a big aggregate. Better a
     * warning in the log months early than a producer error on the one deletion that matters.
     */
    public static final int EXPECTED_MAX_PAYLOAD_CHARS = 1024;

    private final OutboxTable table;
    private final ConnectionSupplier connections;
    private final Clock clock;

    private final String insertSql;
    private final String markPublishedSql;
    private final String selectPendingSql;
    private final String deletePublishedSql;
    private final String recordFailureSql;
    private final String deferRetrySql;
    private final String readAttemptsSql;

    /**
     * @param table       which table (and therefore which SQL) this outbox works on
     * @param connections connections for the non-transactional operations — see
     *                    {@link ConnectionSupplier}
     * @param clock       the clock for {@code created_at} and for both age comparisons. Injected,
     *                    not {@code Clock.systemUTC()}: age is the only thing the republisher
     *                    decides on, so a test that cannot move time can only prove the retention
     *                    and re-send thresholds by sleeping through them.
     */
    public TransactionalOutbox(OutboxTable table, ConnectionSupplier connections, Clock clock) {
        this.table = table;
        this.connections = connections;
        this.clock = clock;
        String name = table.name();
        this.insertSql = "INSERT INTO " + name
                + " (id, topic, event_type, event_key, cid, payload, created_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?)";
        this.markPublishedSql = "UPDATE " + name + " SET published_at = ? WHERE id = ?";
        // Three predicates, and the last two are why poison no longer wedges the queue: a row that
        // just failed is not eligible until its backoff elapses (so it stops occupying the head of
        // an oldest-first batch), and a row that has failed too many times is not eligible at all.
        this.selectPendingSql = "SELECT id, topic, event_type, event_key, cid, payload, attempts FROM "
                + name + " WHERE published_at IS NULL AND created_at <= ?"
                + " AND (next_attempt_at IS NULL OR next_attempt_at <= ?)"
                + " AND attempts < ? ORDER BY created_at, id LIMIT ?";
        this.recordFailureSql = "UPDATE " + name
                + " SET attempts = attempts + 1, next_attempt_at = ? WHERE id = ?";
        this.deferRetrySql = "UPDATE " + name + " SET next_attempt_at = ? WHERE id = ?";
        this.readAttemptsSql = "SELECT attempts FROM " + name + " WHERE id = ?";
        // The IN (SELECT ... LIMIT n) shape is the portable one: PostgreSQL rejects LIMIT on DELETE
        // itself but allows it in a subquery, and H2 in PostgreSQL mode (what the tests and the
        // services' dev profile run) accepts the identical statement — one SQL string for both.
        //
        // The cutoff is on published_at, NOT created_at: the window this reaper implements is "a day
        // after we SENT it", and measuring from creation deleted the trail of anything that had been
        // stuck — which is precisely the trail worth keeping.
        this.deletePublishedSql = "DELETE FROM " + name + " WHERE id IN ("
                + " SELECT id FROM " + name + " WHERE published_at IS NOT NULL AND published_at <= ?"
                + " ORDER BY published_at, id LIMIT ?)";
    }

    /**
     * How long a row waits after its {@code n}-th consecutive failure: exponential from
     * {@code base}, doubling, never past {@code cap}.
     *
     * <p>The point is not politeness towards the broker — it is that a permanently undeliverable row
     * must not keep the head of an oldest-first batch. Without a backoff, {@code resendBatchRows}
     * poisoned rows are enough to make every later event unreachable for ever.
     */
    static Duration backoffAfter(int attempts, Duration base, Duration cap) {
        if (attempts <= 0) {
            return Duration.ZERO;
        }
        // shift rather than Math.pow, and clamped at 30 so the doubling cannot overflow a long
        long multiplier = 1L << Math.min(attempts - 1, 30);
        Duration backoff = base.multipliedBy(multiplier);
        return backoff.compareTo(cap) > 0 ? cap : backoff;
    }

    public OutboxTable table() {
        return table;
    }

    /**
     * Writes the event <strong>in the transaction the given connection is in</strong>. This is
     * property (a) of the whole design: if the caller rolls back, there is no row, and therefore no
     * event — the announcement shares the fate of the change it announces.
     *
     * <p><strong>Why a raw {@link Connection} instead of a framework's transaction abstraction.</strong>
     * The library has consumers on three different frameworks, whose transaction managers agree on
     * nothing except that they all end up handing work to a JDBC connection. Depending on any one of
     * them (a Spring {@code TransactionTemplate}, a container-managed {@code @Transactional}) would
     * make this jar unusable to the other two; depending on all three is not a library any more.
     * {@code Connection} is the narrowest thing that expresses the requirement exactly: "the same
     * unit of work as your business write". The framework wiring — reaching the connection already
     * bound to the ambient transaction — lives in the adapter modules, where it is four lines
     * (Spring: {@code DataSourceUtils.getConnection}).
     *
     * <p><strong>The transaction belongs to the caller.</strong> This method does not commit, does
     * not roll back, does not change autocommit and does not close the connection. Outside a
     * transaction (autocommit) the row simply commits on its own, which is correct too: there is
     * nothing to be atomic with.
     *
     * @throws OutboxException if the row cannot be written — deliberately unchecked so the default
     *                         behaviour is that the caller's transaction rolls back rather than
     *                         committing a change whose announcement was lost. See
     *                         {@link OutboxException}.
     */
    public void append(Connection transaction, OutboxEvent event) {
        if (event.payload().length() > EXPECTED_MAX_PAYLOAD_CHARS) {
            LOG.warn("outbox payload for {} event {} is {} chars — wider than the {} this outbox"
                            + " expects; stored intact (the column is TEXT), but check what grew:"
                            + " a payload that carries a growing list eventually meets the broker's"
                            + " own message-size limit",
                    event.type(), event.id(), event.payload().length(), EXPECTED_MAX_PAYLOAD_CHARS);
        }
        try (PreparedStatement insert = transaction.prepareStatement(insertSql)) {
            insert.setString(1, event.id());
            insert.setString(2, event.topic());
            insert.setString(3, event.type());
            insert.setString(4, event.key());
            insert.setString(5, event.cid());
            insert.setString(6, event.payload());
            insert.setTimestamp(7, Timestamp.from(clock.instant()));
            insert.executeUpdate();
        } catch (SQLException failed) {
            throw new OutboxException("could not append " + event.type() + " event " + event.id()
                    + " to " + table.name() + " — the announcing transaction must not commit", failed);
        }
    }

    /**
     * Marks the row delivered. Called only after a broker CONFIRMATION, never optimistically:
     * this is the library forgetting about the event, and it may only forget what happened.
     *
     * <p>One short UPDATE on its own autocommit connection, so it is safe from the producer's I/O
     * thread. If it fails after a confirmed delivery the event is still delivered — the republisher
     * will re-send a duplicate later, which the event id in the payload makes recognisable. That
     * trade (a rare duplicate) is the right side of the at-least-once/at-most-once choice for every
     * consumer in this estate, all of which are idempotent.
     */
    public void markPublished(String eventId) {
        try (Connection connection = connections.get();
             PreparedStatement mark = connection.prepareStatement(markPublishedSql)) {
            mark.setTimestamp(1, Timestamp.from(clock.instant()));
            mark.setString(2, eventId);
            mark.executeUpdate();
        } catch (SQLException failed) {
            throw new OutboxException("could not mark event " + eventId + " published in "
                    + table.name(), failed);
        }
    }

    /**
     * Holds this row back for a while WITHOUT counting the attempt against it.
     *
     * <p>For the case where the broker never answered. A timeout says nothing about the event, so
     * letting it accumulate towards the poison ceiling turned an outage into permanent data loss:
     * twenty-five rounds of backoff is about eighteen hours, and a broker down for a weekend
     * removed the oldest part of the backlog from the poll for good.
     *
     * <p>The delay is flat rather than exponential, deliberately. Exponential backoff exists to get
     * a hopeless row off the head of an oldest-first batch; a row waiting out an outage is not
     * hopeless, and every row is equally affected, so there is no head to clear. What this does is
     * stop a pass from spinning on the same rows within one interval — nothing more.
     */
    public void deferRetry(String eventId, Duration delay) {
        try (Connection connection = connections.get();
             PreparedStatement defer = connection.prepareStatement(deferRetrySql)) {
            defer.setTimestamp(1, Timestamp.from(clock.instant().plus(delay)));
            defer.setString(2, eventId);
            defer.executeUpdate();
        } catch (SQLException failed) {
            // the row stays unpublished either way; the worst case is one more attempt next pass
            LOG.warn("could not defer the retry of event {} in {}", eventId, table.name(), failed);
        }
    }

    /**
     * Records that an attempt on this row failed: one more attempt, and not eligible again until the
     * backoff elapses.
     *
     * <p>Returns the new attempt count, so the caller can tell a hiccup from poison. The read and the
     * update run on one connection but are deliberately NOT one statement — computing an exponential
     * backoff in SQL is a different expression on every database this library is expected to run on,
     * and this path is rare (it only executes when a send has already failed) and always on the
     * scheduler thread, where a second round trip costs nobody anything.
     */
    public int recordFailedAttempt(String eventId, Duration backoffBase, Duration backoffCap) {
        try (Connection connection = connections.get()) {
            int attempts;
            try (PreparedStatement read = connection.prepareStatement(readAttemptsSql)) {
                read.setString(1, eventId);
                try (ResultSet row = read.executeQuery()) {
                    if (!row.next()) {
                        return 0;      // reaped or never existed: nothing to hold against it
                    }
                    attempts = row.getInt("attempts") + 1;
                }
            }
            try (PreparedStatement record = connection.prepareStatement(recordFailureSql)) {
                record.setTimestamp(1, Timestamp.from(
                        clock.instant().plus(backoffAfter(attempts, backoffBase, backoffCap))));
                record.setString(2, eventId);
                record.executeUpdate();
            }
            return attempts;
        } catch (SQLException failed) {
            // a failure to record a failure must not abort the pass: the row stays unpublished, so
            // the worst case is the old behaviour — it is retried on the next pass without backoff
            LOG.warn("could not record the failed attempt on event {} in {}", eventId, table.name(),
                    failed);
            return 0;
        }
    }

    /**
     * Unpublished events older than {@code minAge} — old enough that a first, after-commit attempt
     * has clearly either crashed or failed, so re-sending them cannot race an attempt still in
     * flight. Oldest first, so a backlog drains in the order it accumulated.
     *
     * @param limit how many rows one pass takes at most. Capped rather than "all of them": the
     *              first pass after a long broker outage would otherwise materialise the entire
     *              backlog in one list and try to deliver it in one pass, on a scheduler thread,
     *              while retention waits behind it. A capped pass drains a backlog over several
     *              passes; steady state never reaches the cap.
     */
    public List<OutboxEvent> pendingOlderThan(Duration minAge, int limit, int poisonAfter) {
        Timestamp now = Timestamp.from(clock.instant());
        Timestamp cutoff = Timestamp.from(clock.instant().minus(minAge));
        List<OutboxEvent> pending = new ArrayList<>();
        try (Connection connection = connections.get();
             PreparedStatement select = connection.prepareStatement(selectPendingSql)) {
            select.setTimestamp(1, cutoff);
            select.setTimestamp(2, now);
            select.setInt(3, poisonAfter);
            select.setInt(4, limit);
            try (ResultSet rows = select.executeQuery()) {
                while (rows.next()) {
                    pending.add(new OutboxEvent(
                            rows.getString("id"),
                            rows.getString("topic"),
                            rows.getString("event_type"),
                            rows.getString("event_key"),
                            rows.getString("cid"),
                            rows.getString("payload")));
                }
            }
        } catch (SQLException failed) {
            throw new OutboxException("could not read overdue events from " + table.name(), failed);
        }
        return pending;
    }

    /**
     * Retention: drops PUBLISHED rows older than {@code retention}, in batches of
     * {@code batchRows} and at most {@code maxBatches} of them, returning how many rows went.
     *
     * <p>A delivered event has no value once its consumers absorbed it — the row existed only to
     * guarantee a delivery that has demonstrably happened — so without a reaper the table grows by
     * one row per event, forever. UNPUBLISHED rows are never touched, however old: age is not
     * expiry, and an old unpublished row still carries an obligation.
     *
     * <p><strong>Why batched and capped, when one statement would read better.</strong> The FIRST
     * pass after this feature ships meets the entire accumulated history of published events. One
     * {@code DELETE ... WHERE published} over that is a single huge transaction on the scheduler
     * thread — a long lock hold, a write-ahead log spike, and a pass that does not return for
     * however long the delete takes, blocking the re-send leg that shares the pass. The cap bounds a
     * pass at {@code maxBatches × batchRows} rows; what does not fit waits for the next pass, so a
     * backlog drains over minutes instead of one stall. Steady state is unaffected — the first batch
     * comes back short and the loop stops.
     *
     * @throws IllegalArgumentException if either limit is not positive — {@code batchRows = 0} would
     *                                 delete nothing while looping, which reads in the log exactly
     *                                 like an empty table
     */
    public int deletePublishedOlderThan(Duration retention, int batchRows, int maxBatches) {
        OutboxDials.positive("retention batch rows", batchRows);
        OutboxDials.positive("retention batches per pass", maxBatches);
        Timestamp cutoff = Timestamp.from(clock.instant().minus(retention));
        int deleted = 0;
        try (Connection connection = connections.get();
             PreparedStatement reap = connection.prepareStatement(deletePublishedSql)) {
            for (int batch = 0; batch < maxBatches; batch++) {
                reap.setTimestamp(1, cutoff);
                reap.setInt(2, batchRows);
                int gone = reap.executeUpdate();
                deleted += gone;
                if (gone < batchRows) {
                    return deleted;   // the table ran out before the cap — nothing left to reap
                }
            }
        } catch (SQLException failed) {
            throw new OutboxException("could not reap delivered events from " + table.name(), failed);
        }
        LOG.info("retention stopped at its cap of {} batch(es) ({} rows) in {} — more delivered rows"
                        + " are past {}, the next pass takes them",
                maxBatches, deleted, table.name(), retention);
        return deleted;
    }
}
