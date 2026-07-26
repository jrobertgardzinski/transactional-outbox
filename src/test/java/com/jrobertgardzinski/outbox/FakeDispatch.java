package com.jrobertgardzinski.outbox;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A {@link Dispatch} the tests steer, standing in for a broker.
 *
 * <p>Hand-written rather than mocked, for one reason that matters here: the interesting behaviours of
 * a broker are TIMING behaviours (an acknowledgement that arrives later, or never), and a stubbed
 * return value cannot express "answers eventually, on someone else's schedule". This fake can hold
 * an outcome open and let a test complete it after asserting that the library did NOT wait for it.
 * It also records every event it was handed, so a test can assert that a redelivery is
 * byte-identical to the first attempt.
 */
final class FakeDispatch implements Dispatch {

    /** What the fake does when handed an event. */
    enum Behaviour {
        /** The broker acknowledges, synchronously (the common happy path). */
        CONFIRM,
        /** The broker refuses (an outage): reported through {@code failed}. */
        REJECT,
        /** The producer refuses the record outright, by throwing — a synchronous send failure. */
        THROW,
        /** Nothing comes back at all. The outcome is parked for the test to settle later. */
        SILENT
    }

    private final List<OutboxEvent> handed = new ArrayList<>();
    private final List<DispatchOutcome> parked = new ArrayList<>();
    private Behaviour behaviour;
    private Consumer<OutboxEvent> onSend = event -> {
    };

    FakeDispatch(Behaviour behaviour) {
        this.behaviour = behaviour;
    }

    void behave(Behaviour next) {
        this.behaviour = next;
    }

    /** A hook that runs INSIDE the send — lets a test observe the database as the broker sees it. */
    void onSend(Consumer<OutboxEvent> hook) {
        this.onSend = hook;
    }

    @Override
    public void send(OutboxEvent event, DispatchOutcome outcome) {
        handed.add(event);
        onSend.accept(event);
        switch (behaviour) {
            case CONFIRM -> outcome.confirmed();
            case REJECT -> outcome.failed(new IllegalStateException("broker down"));
            case THROW -> throw new IllegalStateException("producer closed");
            case SILENT -> parked.add(outcome);
        }
    }

    /** Delivers the acknowledgement a SILENT send never gave — as the producer's I/O thread would. */
    void settleAllConfirmed() {
        List<DispatchOutcome> outstanding = List.copyOf(parked);
        parked.clear();
        outstanding.forEach(DispatchOutcome::confirmed);
    }

    int sends() {
        return handed.size();
    }

    OutboxEvent lastHanded() {
        return handed.getLast();
    }
}
