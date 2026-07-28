package com.jrobertgardzinski.outbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property (f): a misconfigured dial refuses the boot, and the refusal is addressed to the person who
 * set it — the property NAME as they spelled it, and the VALUE they gave.
 *
 * <p>The test asserts the message's content, not just the exception type, because the content is the
 * feature. "IllegalArgumentException: must be positive" in a container log at 3am is a scavenger hunt.
 */
class OutboxDialsTest {

    @Test
    @DisplayName("(f) a retention of zero or less refuses the boot, naming the property and echoing the value")
    void a_non_positive_retention_refuses_to_start() {
        for (long broken : new long[]{0, -1, -24}) {
            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                    () -> OutboxDials.retentionHours("memes.outbox.retention-hours", broken));

            assertTrue(refused.getMessage().contains("memes.outbox.retention-hours"),
                    "the operator who set the dial must read its NAME: " + refused.getMessage());
            assertTrue(refused.getMessage().contains(String.valueOf(broken)),
                    "…and the value they set: " + refused.getMessage());
            assertTrue(refused.getMessage().contains("reap delivered events as fast as they are marked"),
                    "…and WHY it matters, or the fix is a guess: " + refused.getMessage());
        }
    }

    @Test
    @DisplayName("(f) the property name is the SERVICE'S — the library does not own the namespace")
    void the_message_carries_whatever_name_the_service_uses() {
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> OutboxDials.retentionHours("comments.outbox.retention-hours", 0))
                .getMessage().contains("comments.outbox.retention-hours"));
    }

    @Test
    @DisplayName("a valid retention comes back as a duration")
    void a_valid_retention_is_accepted() {
        assertEquals(Duration.ofHours(24), OutboxDials.retentionHours("x.retention-hours", 24));
        assertEquals(Duration.ofHours(1), OutboxDials.retentionHours("x.retention-hours", 1));
    }

    @Test
    @DisplayName("(f) count and duration dials are checked the same way, named and echoed")
    void count_and_duration_dials_are_checked_too() {
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> OutboxDials.positive("memes.outbox.resend-batch-rows", 0))
                .getMessage().contains("memes.outbox.resend-batch-rows"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> OutboxDials.positive("memes.outbox.min-age", Duration.ZERO))
                .getMessage().contains("memes.outbox.min-age"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> OutboxDials.positive("memes.outbox.min-age", Duration.ofSeconds(-5)))
                .getMessage().contains("PT-5S"), "the value must be echoed, negative or not");

        assertEquals(500, OutboxDials.positive("x", 500));
        assertEquals(Duration.ofSeconds(30), OutboxDials.positive("x", Duration.ofSeconds(30)));
    }

    @Test
    @DisplayName("(f) RepublisherSettings is the last line of defence — every dial checked, component named")
    void the_settings_record_validates_every_dial() {
        Duration ok = Duration.ofSeconds(30);
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> new RepublisherSettings(Duration.ZERO, ok, ok, 500, 4, 500, ok, ok, 25))
                .getMessage().contains("minAge"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> new RepublisherSettings(ok, Duration.ZERO, ok, 500, 4, 500, ok, ok, 25))
                .getMessage().contains("confirmationPatience"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> new RepublisherSettings(ok, ok, Duration.ZERO, 500, 4, 500, ok, ok, 25))
                .getMessage().contains("retention"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> new RepublisherSettings(ok, ok, ok, 0, 4, 500, ok, ok, 25))
                .getMessage().contains("retentionBatchRows"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> new RepublisherSettings(ok, ok, ok, 500, 0, 500, ok, ok, 25))
                .getMessage().contains("retentionBatchesPerPass"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> new RepublisherSettings(ok, ok, ok, 500, 4, 0, ok, ok, 25))
                .getMessage().contains("resendBatchRows"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> new RepublisherSettings(ok, ok, ok, 500, 4, 500, Duration.ZERO, ok, 25))
                .getMessage().contains("backoffBase"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> new RepublisherSettings(ok, ok, ok, 500, 4, 500, ok, Duration.ZERO, 25))
                .getMessage().contains("backoffCap"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> new RepublisherSettings(ok, ok, ok, 500, 4, 500, ok, ok, 0))
                .getMessage().contains("poisonAfter"));
    }

    @Test
    @DisplayName("the defaults are the values the reference implementation ran, with only retention left open")
    void the_defaults_are_the_hardened_ones() {
        RepublisherSettings defaults = RepublisherSettings.defaults(Duration.ofHours(24));

        // 60s, not the 30s this shipped with: both services run a 30s delivery.timeout.ms, and the
        // age clock starts at created_at — stamped INSIDE the announcing transaction, before the
        // first attempt can even begin. At 30s the margin was zero minus the transaction's duration,
        // so the republisher routinely sent a second copy while the first sat in the accumulator.
        assertEquals(Duration.ofSeconds(60), defaults.minAge());
        assertEquals(Duration.ofSeconds(5), defaults.confirmationPatience());
        assertEquals(Duration.ofHours(24), defaults.retention());
        assertEquals(500, defaults.retentionBatchRows());
        assertEquals(4, defaults.retentionBatchesPerPass());
        assertEquals(500, defaults.resendBatchRows());
        assertEquals(Duration.ofSeconds(30), defaults.backoffBase());
        assertEquals(Duration.ofHours(1), defaults.backoffCap());
        assertEquals(25, defaults.poisonAfter());
    }
}
