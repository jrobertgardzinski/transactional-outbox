package com.jrobertgardzinski.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The outbox's second leg, and the half that actually makes the promise true: re-send whatever was
 * written but never confirmed — a crash between the commit and the send, a broker outage during it —
 * and forget what has demonstrably been delivered.
 *
 * <p>Framework-free: {@link #runOnce()} is one pass, and WHAT calls it every few seconds is the
 * host's business (a Spring {@code @Scheduled} bean, a Micronaut {@code @Scheduled}, a plain
 * {@code ScheduledExecutorService}, or a test calling it directly). That is the whole reason a pass
 * is a method and not a loop with a sleep in it.
 *
 * <p>A pass never throws. A database hiccup or an unconfirmed send costs one pass, not an event: the
 * rows are still there and the next pass finds them. Throwing would be worse than useless — most
 * schedulers cancel a task whose invocation threw.
 */
public final class OutboxRepublisher {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRepublisher.class);

    /** What one pass did — returned so a test can assert on the pass rather than on the log. */
    public record Pass(int marked, int reaped, int resent, int confirmed, int poisoned) {
        static final Pass NOTHING = new Pass(0, 0, 0, 0, 0);
    }

    private final TransactionalOutbox outbox;
    private final OutboxPublisher publisher;
    private final RepublisherSettings settings;

    public OutboxRepublisher(TransactionalOutbox outbox, OutboxPublisher publisher,
                             RepublisherSettings settings) {
        this.outbox = outbox;
        this.publisher = publisher;
        this.settings = settings;
    }

    /**
     * One pass: retention first, re-send second.
     *
     * <p><strong>The order is a guarantee, not a preference.</strong> A row that THIS pass delivers
     * gets its {@code published} mark seconds before retention would look at it; reaping after
     * sending would put the reap and the mark in the same breath, and a retention window
     * misconfigured to near-zero would then delete the row of an event whose confirmation is still
     * in flight — losing the record of a delivery that may not have happened. Reaping first means
     * every row this pass marks survives until the NEXT pass finds it genuinely aged out.
     *
     * <p>The re-send leg accepts duplicates by design. A confirmed send whose mark did not land is
     * re-sent, and that is the right trade: the alternative (mark first, then send) turns every crash
     * into a silently lost event, while a duplicate is recognisable from the event id in the payload
     * and absorbed by consumers that must be idempotent anyway — the redelivery they already have to
     * survive from at-least-once broker semantics.
     */
    public Pass runOnce() {
        try {
            // First: the marks the producer's I/O thread was not allowed to write itself. This runs
            // before retention on purpose — a row confirmed seconds ago gets its published_at now,
            // and since retention measures from published_at it cannot be reaped for a whole window.
            int marked = publisher.drainConfirmations();

            int reaped = outbox.deletePublishedOlderThan(settings.retention(),
                    settings.retentionBatchRows(), settings.retentionBatchesPerPass());
            if (reaped > 0) {
                LOG.info("dropped {} delivered event(s) older than {} from {}", reaped,
                        settings.retention(), outbox.table().name());
            }

            List<OutboxEvent> overdue = outbox.pendingOlderThan(settings.minAge(),
                    settings.resendBatchRows(), settings.poisonAfter());
            if (overdue.isEmpty()) {
                return new Pass(marked, reaped, 0, 0, 0);
            }
            LOG.warn("re-sending {} unconfirmed event(s) from {}", overdue.size(),
                    outbox.table().name());
            int confirmed = 0;
            int poisoned = 0;
            for (OutboxEvent event : overdue) {
                OutboxPublisher.Attempt outcome =
                        publisher.publishAndWait(event, settings.confirmationPatience());
                if (outcome == OutboxPublisher.Attempt.CONFIRMED) {
                    confirmed++;
                    continue;
                }
                if (outcome == OutboxPublisher.Attempt.TIMED_OUT) {
                    // The broker said nothing — which is what an outage looks like, and says nothing
                    // about this event. Counting it towards the ceiling meant a weekend-long outage
                    // permanently removed the oldest backlog from the poll. Hold the row back, do
                    // not hold it against it.
                    outbox.deferRetry(event.id(), settings.backoffBase());
                    continue;
                }
                // REJECTED: the broker ANSWERED, and its answer was no. That is about this event —
                // a payload past the message limit, a missing topic, an ACL — so it counts. Without
                // this an undeliverable event was retried every interval for ever AND, because the
                // poll is oldest-first, it stood at the head of the queue.
                int attempts = outbox.recordFailedAttempt(event.id(), settings.backoffBase(),
                        settings.backoffCap());
                if (attempts >= settings.poisonAfter()) {
                    poisoned++;
                    // NOT the payload. A USER_CONTENT_PURGED body carries the e-mail of the person
                    // whose data is being erased, and both participants promise in writing that
                    // "the logs, which have no retention anyone controls, still never see it".
                    // The row is right there, unpublished, and the id finds it.
                    LOG.error("event {} ({} for {}) has failed {} times and will NOT be retried again"
                                    + " — it stays in {} unpublished, under that id, and a human has"
                                    + " to decide what to do with it",
                            event.id(), event.type(), event.key(), attempts, outbox.table().name());
                }
            }
            return new Pass(marked, reaped, overdue.size(), confirmed, poisoned);
        } catch (RuntimeException passFailed) {
            // Deliberately swallowed: most schedulers stop invoking a task whose run threw, which
            // would silently retire the durability mechanism after one bad database moment.
            LOG.error("outbox pass over {} failed — nothing is lost, the rows stay and the next pass"
                    + " retries", outbox.table().name(), passFailed);
            return Pass.NOTHING;
        }
    }
}
