package com.jrobertgardzinski.outbox;

/**
 * How a {@link Dispatch} reports back. Exactly one of the two methods is called, exactly once, and
 * whichever thread the producer's callback runs on is fine — the library's reaction to either is a
 * single short statement or a log line.
 *
 * <p>Two methods rather than {@code accept(boolean, Throwable)}: the difference between "delivered"
 * and "not delivered" is the difference between forgetting the row and keeping it, so it should not
 * be possible to express by accident. A boolean flag invites {@code accept(true, someError)}.
 */
public interface DispatchOutcome {

    /**
     * The BROKER acknowledged the event. This — and only this — is what allows the row to be marked
     * published, i.e. what allows the library to stop guaranteeing the event.
     */
    void confirmed();

    /**
     * The event did not get through (refused, timed out, broker error). Costs nothing durable: the
     * row stays unpublished and {@link OutboxRepublisher} will retry it. The cause is logged, not
     * rethrown — there is nobody left to tell on the after-commit path.
     */
    void failed(Throwable cause);
}
