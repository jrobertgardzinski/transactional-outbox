package com.jrobertgardzinski.outbox;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/**
 * A clock the tests move by hand. Age is the only thing the republisher decides on — "older than
 * minAge", "older than retention" — so a test that could not move time could only prove those
 * thresholds by sleeping through them.
 *
 * <p>Deliberately a local 20-liner rather than a dependency on the kernel's {@code adjustable-clock}:
 * the library must stay buildable and testable on its own, with nothing but the JDK, JUnit and a
 * JDBC driver on the classpath.
 */
final class SteerableClock extends Clock {

    private Instant now;

    SteerableClock(Instant start) {
        this.now = start;
    }

    static SteerableClock at(String instant) {
        return new SteerableClock(Instant.parse(instant));
    }

    void advance(Duration by) {
        now = now.plus(by);
    }

    @Override
    public Instant instant() {
        return now;
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException("the tests only ever need UTC");
    }
}
