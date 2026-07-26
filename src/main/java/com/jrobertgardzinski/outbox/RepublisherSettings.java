package com.jrobertgardzinski.outbox;

import java.time.Duration;

/**
 * The republisher's five dials, validated on construction.
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
 *                                comfortably above that timeout.
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
 */
public record RepublisherSettings(
        Duration minAge,
        Duration confirmationPatience,
        Duration retention,
        int retentionBatchRows,
        int retentionBatchesPerPass,
        int resendBatchRows) {

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
    }

    /**
     * The values the reference implementation ran in production, with only the retention window left
     * to the service: 30s minimum age, 5s confirmation patience, 500-row retention batches, at most
     * 4 of them per pass (2000 rows per pass — a backlog drains at that rate), 500 rows re-sent per
     * pass. A service with different clocks should say so explicitly through the canonical
     * constructor rather than quietly diverge from these.
     */
    public static RepublisherSettings defaults(Duration retention) {
        return new RepublisherSettings(Duration.ofSeconds(30), Duration.ofSeconds(5), retention,
                500, 4, 500);
    }
}
