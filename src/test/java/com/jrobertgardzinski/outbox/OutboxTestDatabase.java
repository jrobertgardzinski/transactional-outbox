package com.jrobertgardzinski.outbox;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A real database for the library's own tests — H2 in <strong>PostgreSQL mode</strong>, the dialect
 * the estate's services run their JDBC adapters against, and created from
 * {@link OutboxTable#ddl()} itself.
 *
 * <p>That last detail is the point of the fixture: the DDL this library DOCUMENTS is the DDL its
 * tests RUN, so the shape a service pastes into its Flyway migration cannot silently drift away from
 * the shape the queries expect. A renamed column breaks the suite instead of production.
 *
 * <p>No framework, no container, no service: a JDBC URL and {@code DriverManager}. Each instance
 * gets its own in-memory database so tests cannot see each other's rows.
 */
final class OutboxTestDatabase {

    private static final AtomicLong DATABASES = new AtomicLong();

    private final String url;
    private final OutboxTable table;

    OutboxTestDatabase(String tableName) {
        this.url = "jdbc:h2:mem:outbox-test-" + DATABASES.incrementAndGet()
                + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE";
        this.table = OutboxTable.named(tableName);
        // the documented DDL, verbatim — including the index and the comments. Split on ';' the way
        // Flyway would: the point is that each statement the library documents really executes.
        for (String statement : table.ddl().split(";")) {
            if (!statement.isBlank()) {
                run(statement);
            }
        }
    }

    OutboxTable table() {
        return table;
    }

    /** The supplier the outbox uses for its non-transactional legs: a fresh autocommit connection. */
    ConnectionSupplier connections() {
        return () -> DriverManager.getConnection(url);
    }

    /** A connection the CALLER owns and manages a transaction on — {@code append}'s seam. */
    Connection openTransaction() throws SQLException {
        Connection connection = DriverManager.getConnection(url);
        connection.setAutoCommit(false);
        return connection;
    }

    void run(String sql) {
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException failed) {
            throw new IllegalStateException("test fixture SQL failed: " + sql, failed);
        }
    }

    int count(String whereClause, Object... params) {
        return queryOne("SELECT COUNT(*) FROM " + table.name() + " WHERE " + whereClause, params,
                rs -> rs.getInt(1));
    }

    int rows() {
        return queryOne("SELECT COUNT(*) FROM " + table.name(), new Object[0], rs -> rs.getInt(1));
    }

    boolean published(String eventId) {
        // published_at replaced a boolean on 2026-07-28 so retention could measure its window from
        // the DELIVERY rather than from the creation; NULL still means "owed", exactly as FALSE did
        return queryOne("SELECT published_at FROM " + table.name() + " WHERE id = ?",
                new Object[]{eventId}, rs -> rs.getTimestamp(1) != null);
    }

    String payload(String eventId) {
        return queryOne("SELECT payload FROM " + table.name() + " WHERE id = ?",
                new Object[]{eventId}, rs -> rs.getString(1));
    }

    String cid(String eventId) {
        return queryOne("SELECT cid FROM " + table.name() + " WHERE id = ?",
                new Object[]{eventId}, rs -> rs.getString(1));
    }

    /**
     * Ages rows — the tests' alternative to sleeping.
     *
     * <p>Shifts {@code published_at} as well as {@code created_at}, because retention measures its
     * window from the delivery. Moving only the creation would age a row for the RE-SEND leg while
     * leaving it forever fresh for the reaper, which is a state the production code cannot produce
     * and would make these tests prove the wrong thing.
     */
    void backdateBySeconds(String eventKey, long seconds) {
        try (Connection connection = DriverManager.getConnection(url);
             PreparedStatement update = connection.prepareStatement("UPDATE " + table.name()
                     + " SET created_at = DATEADD('SECOND', ?, created_at),"
                     + " published_at = DATEADD('SECOND', ?, published_at) WHERE event_key = ?")) {
            update.setLong(1, -seconds);
            update.setLong(2, -seconds);
            update.setString(3, eventKey);
            update.executeUpdate();
        } catch (SQLException failed) {
            throw new IllegalStateException("could not backdate " + eventKey, failed);
        }
    }

    private interface RowReader<T> {
        T read(ResultSet rs) throws SQLException;
    }

    private <T> T queryOne(String sql, Object[] params, RowReader<T> reader) {
        try (Connection connection = DriverManager.getConnection(url);
             PreparedStatement query = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                query.setObject(i + 1, params[i]);
            }
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) {
                    throw new IllegalStateException("no row for: " + sql);
                }
                return reader.read(rows);
            }
        } catch (SQLException failed) {
            throw new IllegalStateException("test fixture query failed: " + sql, failed);
        }
    }
}
