package com.jrobertgardzinski.outbox;

import java.sql.SQLException;

/**
 * A database failure inside the outbox, unchecked on purpose.
 *
 * <p>The choice matters most on the write path. {@link TransactionalOutbox#append} runs inside the
 * caller's business transaction, and if the row cannot be written the announcement is not durable —
 * so the transaction MUST NOT commit. An unchecked throw does that by default: it propagates out of
 * the caller's transactional method and the transaction manager rolls back. A checked
 * {@link SQLException} would instead force every caller to write a catch block, and the tempting
 * body of that block ("log it and carry on") is exactly the silent data loss this library exists to
 * remove. Making the safe behaviour the DEFAULT one is worth losing the compiler's reminder.
 *
 * <p>On the republisher's paths the same throw is caught and logged by {@link OutboxRepublisher} —
 * a database hiccup there costs one pass, not a lost event: the row is still owed and the next pass
 * finds it.
 */
public class OutboxException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public OutboxException(String message, Throwable cause) {
        super(message, cause);
    }
}
