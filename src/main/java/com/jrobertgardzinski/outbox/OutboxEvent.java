package com.jrobertgardzinski.outbox;

import java.util.UUID;

/**
 * One fully built, ready-to-send event — everything the producer will need, captured at ANNOUNCE
 * time and stored verbatim. A republication hours later rebuilds the exact same record from the
 * row, on a scheduler thread that has none of the announcing request's context left.
 *
 * <p><strong>Why the payload is a {@code String} and not a tree, a map or a typed envelope.</strong>
 * The library's promise is delivery, not meaning: it never reads the payload, never validates it,
 * never re-serialises it. Keeping it opaque buys three things. The redelivery is byte-identical to
 * the first attempt by construction (nothing can re-order a map's keys or drop a field between the
 * two). The library needs no JSON dependency, so it stays framework- and format-free — a service
 * publishing Avro, protobuf or a bare id puts its bytes in the same column. And a service keeps
 * ownership of its own envelope version (workspace ADR 0004), which is a contract with its
 * consumers, not with this library.
 *
 * <p><strong>{@link #id()} is also the event's identity on the wire.</strong> Redelivery is
 * possible by design — a confirmed send whose {@code published} mark did not land is re-sent by
 * {@link OutboxRepublisher} — so a consumer must be able to recognise the duplicate. The service
 * therefore embeds THIS id in the payload (see {@link #newId()}); the library cannot do it,
 * because it does not know the payload's shape. That is a documented service responsibility.
 *
 * @param id      the row's primary key AND the event id the service embeds in {@code payload}
 * @param topic   destination the {@link Dispatch} sends to (the library never guesses it)
 * @param type    event type, stored for operators reading the table and for the payload canary's
 *                log line — the library itself does not route on it
 * @param key     partition key: everything about one aggregate stays on one partition, so a
 *                consumer never sees a later hop of a cascade before an earlier one
 * @param cid     correlation id captured at announce time, or {@code null}. STORED rather than
 *                read from a thread-local at send time — see {@link Dispatch}.
 * @param payload the bytes to deliver, opaque to this library
 */
public record OutboxEvent(String id, String topic, String type, String key, String cid, String payload) {

    public OutboxEvent {
        id = required("id", id);
        topic = required("topic", topic);
        type = required("type", type);
        key = required("key", key);
        payload = required("payload", payload);
        // cid stays nullable: not every announcement happens inside a traced request (a saga
        // reacting to a broker record, a scheduled sweep), and a missing trace must not be a
        // reason to refuse a durable event.
    }

    private static String required(String field, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("outbox event " + field + " must not be blank");
        }
        return value;
    }

    /**
     * A fresh event id, in the shape the documented table expects ({@code varchar(36)}).
     *
     * <p>Random, not derived from the payload: two deletions of the same aggregate are two
     * distinct obligations, and content-addressing would collapse them. "Deterministic" in this
     * design means something else and stricter — that the id, once minted, is the SAME in the row
     * and in the payload, so every redelivery of that row repeats the identical event. The service
     * gets the id from here first and builds the payload around it.
     */
    public static String newId() {
        return UUID.randomUUID().toString();
    }
}
