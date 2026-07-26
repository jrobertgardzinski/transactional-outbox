package com.jrobertgardzinski.outbox;

import java.time.Duration;

/**
 * Range checks for the outbox's configuration dials — refusing the boot rather than the surprise.
 *
 * <p>Every method here NAMES the property and ECHOES the value. That is not politeness, it is the
 * whole point: the person who reads the message is the operator who set the environment variable,
 * not the developer who wrote this class, and "must be positive" without a name sends them grepping
 * a codebase they do not have. Same contract as the estate's other dial checks.
 *
 * <p>The property name is a PARAMETER because the library does not own the namespace. The dial that
 * a service exposes as {@code memes.outbox.retention-hours} (env {@code MEMES_OUTBOX_RETENTION_HOURS})
 * is the same dial another exposes under its own prefix; only the service knows which string its
 * operator typed.
 */
public final class OutboxDials {

    private OutboxDials() {
    }

    /**
     * The retention window, from an hours dial. Non-positive is refused loudly: a retention of 0
     * would reap every delivered row the instant the pass runs — including one that a concurrent
     * first attempt is about to mark published — and quietly cost the outbox the forensic window its
     * whole value rests on. A negative one is worse arithmetic dressed as the same mistake.
     *
     * @param propertyName the dial as the operator spells it, e.g. {@code memes.outbox.retention-hours}
     * @param hours        the configured value
     */
    public static Duration retentionHours(String propertyName, long hours) {
        if (hours <= 0) {
            throw new IllegalArgumentException(propertyName + " must be at least 1 (a retention of "
                    + hours + " would reap delivered events as fast as they are marked), was " + hours);
        }
        return Duration.ofHours(hours);
    }

    /**
     * Any strictly positive count dial (batch sizes, batches per pass, re-send limits).
     *
     * @param propertyName the dial as the operator spells it
     */
    public static int positive(String propertyName, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(propertyName + " must be greater than zero"
                    + " (a non-positive limit makes the pass do nothing while looking busy), was "
                    + value);
        }
        return value;
    }

    /**
     * Any strictly positive duration dial (minimum re-send age, confirmation patience).
     *
     * @param propertyName the dial as the operator spells it
     */
    public static Duration positive(String propertyName, Duration value) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(propertyName + " must be a positive duration, was "
                    + value);
        }
        return value;
    }
}
