package com.jrobertgardzinski.outbox;

import java.time.Duration;

/**
 * The republisher's dials, validated on construction.
 *
 * <p>A record rather than five constructor parameters on {@link OutboxRepublisher}: five values of
 * which three are counts and two are durations is exactly the signature where a caller silently
 * swaps two arguments. It also gives the service one object to build from its own configuration,
 * and one place where the defaults below are written down with their reasons.
 *
 * @param minAge                  how old an unpublished row must be before the republisher touches
 *                                it. Its only job is to not race a first attempt that is still in
 *                                flight: below the producer's own delivery timeout, a re-send would
 *                                duplicate events routinely instead of exceptionally. Keep it
 *                                comfortably above that timeout — and note that the age is measured
 *                                from {@code created_at}, which is stamped INSIDE the announcing
 *                                transaction, before the first attempt can even start. The real
 *                                margin is therefore {@code minAge - deliveryTimeout - transaction
 *                                duration}; at the old default of 30s against a 30s delivery timeout
 *                                it was zero minus the transaction, so a slow broker plus a long
 *                                cascade produced duplicates as a matter of routine.
 * @param confirmationPatience    how long the republisher's send waits for the broker's
 *                                acknowledgement before giving up on this pass. Blocking is FREE
 *                                here — nobody waits on the scheduler thread — which is why the
 *                                republisher, not the first attempt, is the durability mechanism.
 *                                Worth spelling as the same number as the producer's own
 *                                "how long do we wait for a broker" dial, so the service has one
 *                                answer to that question rather than two.
 * @param retention               how long a DELIVERED row is kept before being reaped. It buys a
 *                                forensic window ("did we really send that, and when?"), so it is
 *                                measured in hours, not seconds.
 * @param retentionBatchRows      rows per retention statement. Small enough that the statement's
 *                                transaction is short (a handful of pages, locks held for
 *                                milliseconds), large enough that a normal pass is one or two
 *                                statements.
 * @param retentionBatchesPerPass how many such statements one pass may run — the ceiling that keeps
 *                                the first pass over a long-accumulated table from becoming one long
 *                                transaction that also delays the pass's re-send leg.
 * @param resendBatchRows         how many overdue rows one pass tries to deliver. Bounds the pass
 *                                after a long outage; the rest goes out on the following passes.
 * @param backoffBase             how long a row waits after its FIRST failed attempt; doubles with
 *                                every further failure. Not politeness towards the broker — it is
 *                                what stops an undeliverable row from permanently occupying the head
 *                                of an oldest-first batch.
 * @param backoffCap              the ceiling that doubling never passes, so a row that has been
 *                                failing for a day is still retried within the hour.
 * @param poisonAfter             how many failed attempts before a row stops being polled at all. It
 *                                stays in the table, unpublished — nothing is deleted and nothing is
 *                                lost — but the pass no longer spends its patience on an event that
 *                                has proven it cannot be sent (a payload past the broker's message
 *                                limit, a deleted topic, an ACL error). Crossing this line is an
 *                                ERROR in the log, because it means an event will NOT arrive and a
 *                                human has to decide what to do about it.
 */
public record RepublisherSettings(
        Duration minAge,
        Duration confirmationPatience,
        Duration retention,
        int retentionBatchRows,
        int retentionBatchesPerPass,
        int resendBatchRows,
        Duration backoffBase,
        Duration backoffCap,
        int poisonAfter) {

    /**
     * @throws IllegalArgumentException if any dial is not positive. Last line of defence, naming the
     *                                 record component; a service that reads a dial from
     *                                 configuration should validate it first with {@link OutboxDials},
     *                                 which can name the property as the operator spelled it.
     */
    public RepublisherSettings {
        OutboxDials.positive("minAge", minAge);
        OutboxDials.positive("confirmationPatience", confirmationPatience);
        OutboxDials.positive("retention", retention);
        OutboxDials.positive("retentionBatchRows", retentionBatchRows);
        OutboxDials.positive("retentionBatchesPerPass", retentionBatchesPerPass);
        OutboxDials.positive("resendBatchRows", resendBatchRows);
        OutboxDials.positive("backoffBase", backoffBase);
        OutboxDials.positive("backoffCap", backoffCap);
        OutboxDials.positive("poisonAfter", poisonAfter);
    }

    /**
     * The values the reference implementation ran in production, with only the retention window left
     * to the service: 60s minimum age, 5s confirmation patience, 500-row retention batches, at most
     * 4 of them per pass (2000 rows per pass — a backlog drains at that rate), 500 rows re-sent per
     * pass, a 30s backoff doubling to at most an hour, and 25 attempts before a row is left alone.
     * A service with different clocks should say so explicitly through the canonical constructor
     * rather than quietly diverge from these.
     *
     * <p>minAge was 30s until 2026-07-28 and is 60s now: both services run a 30s
     * {@code delivery.timeout.ms}, and since the age clock starts inside the announcing transaction,
     * 30s left no margin at all — the republisher routinely sent a second copy while the first was
     * still sitting in the producer's accumulator. 60s is two delivery timeouts.
     */
    public static RepublisherSettings defaults(Duration retention) {
        return new RepublisherSettings(Duration.ofSeconds(60), Duration.ofSeconds(5), retention,
                500, 4, 500, Duration.ofSeconds(30), Duration.ofHours(1), 25);
    }
}
