package com.jrobertgardzinski.outbox;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Where the outbox gets a connection for the work that must NOT join the caller's transaction:
 * marking a row published, polling for overdue rows, reaping delivered ones.
 *
 * <p><strong>Why those three need their own connection.</strong> The mark runs on the producer's
 * I/O thread when the broker's acknowledgement arrives — a thread that has no transaction and must
 * not touch the (by then committed, possibly still bound) transaction of the thread that announced.
 * The republisher's two legs run on a scheduler thread and want SHORT, independent statements: one
 * long transaction spanning a whole retention pass would hold locks for as long as the pass takes.
 *
 * <p>Contract: return a usable connection in <strong>autocommit</strong> mode. The outbox closes
 * what it borrows here (try-with-resources), which for a pooled {@code DataSource} returns it to
 * the pool. An implementation is usually a method reference — {@code dataSource::getConnection}.
 *
 * <p>Note what this is NOT: the seam for {@link TransactionalOutbox#append}. That one takes an
 * explicit {@link Connection} instead, because the transaction there belongs to the caller — see
 * that method's javadoc.
 */
@FunctionalInterface
public interface ConnectionSupplier {

    Connection get() throws SQLException;
}
