package com.jrobertgardzinski.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Delivery with the marking discipline: <strong>a row is marked published only after the broker
 * confirmed it</strong>. Both legs of the outbox go through here, and they differ in exactly one
 * respect — whether the calling thread waits for that confirmation.
 *
 * <p><strong>Why two methods and not one.</strong> The first attempt runs on the thread that
 * announced (a request thread, or a broker listener thread driving a cascade of N announcements) and
 * must not wait: with the broker down, waiting costs that thread N &times; patience, which overruns
 * a consumer's {@code max.poll.interval.ms} and turns a broker outage into a consumer-group
 * rebalance. The republisher's attempt runs on a scheduler thread where nobody is waiting, so it CAN
 * wait — and it must, because waiting is what lets it mark the row correctly. Hence: the first
 * attempt is best effort, the republisher is the guarantee.
 *
 * <p>Neither method throws. After the announcing transaction has committed there is no caller left
 * to tell, and inside a republisher pass a failure on one event must not cut the pass short for the
 * rest. Every failure path ends the same way: the row stays unpublished, and the next pass tries
 * again.
 */
public final class OutboxPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxPublisher.class);

    private final TransactionalOutbox outbox;
    private final Dispatch dispatch;

    /**
     * Ids confirmed on the producer's own I/O thread, waiting to be marked by someone who is allowed
     * to block.
     *
     * <p>The mark used to run right there in the callback, and that callback is the Kafka producer's
     * single sender thread. {@code connections.get()} on a pool with nothing free blocks — Hikari's
     * default is thirty seconds — so a database under strain froze ALL of the service's Kafka I/O:
     * unrelated sends hit their own delivery timeout, which created more unpublished outbox rows,
     * which needed more marking. A database problem was promoted into a Kafka availability problem,
     * with a feedback loop, at exactly the moment the database was already suffering.
     *
     * <p>Unbounded on purpose: what accumulates here is bounded by throughput times the republisher's
     * interval (seconds), and the failure mode of dropping an id is a guaranteed duplicate later,
     * which is strictly worse than holding a string.
     */
    private final Queue<String> confirmedAwaitingMark = new ConcurrentLinkedQueue<>();

    public OutboxPublisher(TransactionalOutbox outbox, Dispatch dispatch) {
        this.outbox = outbox;
        this.dispatch = dispatch;
    }

    /**
     * Marks everything the producer confirmed since the last call. Driven by the republisher's pass,
     * i.e. on the scheduler thread, where blocking on a connection harms nothing.
     *
     * @return how many rows were marked
     */
    public int drainConfirmations() {
        int marked = 0;
        for (String eventId = confirmedAwaitingMark.poll(); eventId != null;
             eventId = confirmedAwaitingMark.poll()) {
            try {
                outbox.markPublished(eventId);
                marked++;
            } catch (RuntimeException markFailed) {
                // delivered but unmarked: the republisher re-sends a duplicate later, which every
                // consumer in this estate already has to absorb from at-least-once semantics
                LOG.warn("event {} WAS delivered but could not be marked published — the republisher"
                        + " may re-send a harmless duplicate", eventId, markFailed);
            }
        }
        return marked;
    }

    /**
     * The FIRST delivery attempt: hand the event over and return — the announcing thread never waits
     * for the broker's acknowledgement. The mark rides the confirmation callback instead, so a
     * happy-path event is still published exactly once and never re-sent.
     *
     * <p>Call this AFTER the announcing transaction has committed (the Spring adapter parks it on a
     * transaction synchronisation; outside a transaction it can run immediately). Calling it before
     * the commit would publish an event that a rollback then un-announces — the one failure mode the
     * outbox exists to make impossible.
     *
     * <p>If the confirmation never arrives, nothing happens and nothing is lost: the row stays
     * unpublished for {@link OutboxRepublisher}.
     */
    public void publishWithoutWaiting(OutboxEvent event) {
        try {
            dispatch.send(event, new DispatchOutcome() {
                @Override
                public void confirmed() {
                    // NOT outbox.markPublished: this runs on the producer's I/O thread and the mark
                    // is JDBC. See confirmedAwaitingMark for what that used to cost.
                    confirmedAwaitingMark.add(event.id());
                }

                @Override
                public void failed(Throwable cause) {
                    LOG.error("event {} ({} for {}) was not confirmed by the broker — the outbox"
                                    + " keeps it, the republisher will retry",
                            event.id(), event.type(), event.key(), cause);
                }
            });
        } catch (RuntimeException sendRefused) {
            // A producer can also fail synchronously (closed, buffer full, serialisation). Same
            // contract as a reported failure: log, the row stays, the republisher retries.
            LOG.error("event {} ({} for {}) could not even be handed to the producer — the outbox"
                    + " keeps it, the republisher will retry", event.id(), event.type(), event.key(),
                    sendRefused);
        }
    }

    /**
     * How an attempt ended, and the distinction matters more than it looks.
     *
     * <p>{@link #REJECTED} is about THIS event: a payload past the broker's message limit, a topic
     * that does not exist, an ACL refusal. Trying it again will fail again, so it should count
     * towards giving up. {@link #TIMED_OUT} is about the BROKER: it said nothing within the
     * patience window, which is what an outage looks like and says nothing at all about the event.
     *
     * <p>Collapsing the two into one boolean meant a long outage counted every row towards the
     * poison ceiling. Twenty-five rounds of backoff is about eighteen hours, so a broker down over
     * a weekend permanently removed the oldest part of the backlog from the poll — the library's
     * one promise, "will eventually reach the broker", quietly broken by the very mechanism added
     * to protect it, with no way back except a hand-written UPDATE.
     */
    public enum Attempt { CONFIRMED, TIMED_OUT, REJECTED }

    /**
     * The REPUBLISHER'S delivery attempt: send, WAIT for the confirmation up to {@code patience},
     * and only then mark the row published. Delivered-first — the guarantee lives here.
     *
     * <p>The wait is built on top of the same asynchronous {@link Dispatch} the first attempt uses,
     * deliberately: one send path in the service means the two legs cannot drift apart (different
     * headers, different topic, a fix applied to one and not the other). The blocking is the
     * library's, not the service's.
     *
     * @return how the attempt ended. Anything other than {@link Attempt#CONFIRMED} means "still
     *         owed", and the row is untouched for the next pass — but see {@link Attempt} for why
     *         the caller must not treat the two failures alike.
     */
    public Attempt publishAndWait(OutboxEvent event, Duration patience) {
        // null completion = confirmed; a non-null one carries the failure. One future, so the
        // outcome can only be observed once even if a misbehaving Dispatch reports twice.
        CompletableFuture<Throwable> settled = new CompletableFuture<>();
        try {
            dispatch.send(event, new DispatchOutcome() {
                @Override
                public void confirmed() {
                    settled.complete(null);
                }

                @Override
                public void failed(Throwable cause) {
                    settled.complete(cause != null ? cause
                            : new IllegalStateException("dispatch reported a failure without a cause"));
                }
            });
        } catch (RuntimeException sendRefused) {
            // the producer refused it outright (closed, buffer full, serialisation) — about the
            // event or about this instance, not about the broker's availability
            LOG.error("event {} ({} for {}) could not be handed to the producer on re-send — it stays"
                    + " in the outbox", event.id(), event.type(), event.key(), sendRefused);
            return Attempt.REJECTED;
        }

        Throwable failure;
        try {
            failure = settled.get(patience.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            LOG.error("interrupted before event {} ({} for {}) was confirmed — it stays in the outbox",
                    event.id(), event.type(), event.key(), interrupted);
            return Attempt.TIMED_OUT;   // a shutdown says nothing about the event
        } catch (Exception notConfirmed) {
            // TimeoutException or an ExecutionException from the outcome's own completion: either
            // way the broker did not say yes within the patience, so the row keeps its obligation.
            LOG.error("event {} ({} for {}) was not confirmed by the broker within {} — it stays in"
                            + " the outbox, the next pass retries",
                    event.id(), event.type(), event.key(), patience, notConfirmed);
            return Attempt.TIMED_OUT;
        }
        if (failure != null) {
            LOG.error("event {} ({} for {}) was rejected by the broker — it stays in the outbox,"
                    + " the next pass retries", event.id(), event.type(), event.key(), failure);
            return Attempt.REJECTED;   // the broker ANSWERED, and its answer was no
        }
        markDelivered(event);
        return Attempt.CONFIRMED;
    }

    /**
     * The mark, isolated from the delivery it records. A failure here is NOT a delivery failure —
     * the event went out — so it may not be reported as one: the truthful consequence is a possible
     * duplicate on a later pass, which the event id in the payload makes recognisable to an
     * idempotent consumer.
     */
    private void markDelivered(OutboxEvent event) {
        try {
            outbox.markPublished(event.id());
        } catch (RuntimeException markFailed) {
            LOG.warn("event {} ({} for {}) WAS delivered but could not be marked published — the"
                            + " republisher may re-send a harmless duplicate",
                    event.id(), event.type(), event.key(), markFailed);
        }
    }
}
