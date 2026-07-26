package com.jrobertgardzinski.outbox;

/**
 * The send, supplied by the service. The library stores, times and marks; it does not know what a
 * broker is.
 *
 * <p><strong>Why sending is an interface and not a dependency.</strong> The estate's producers are
 * not interchangeable: one service holds a Spring {@code KafkaTemplate}, another a raw
 * {@code KafkaProducer}, a third could publish over AMQP or an HTTP webhook. Pulling
 * {@code kafka-clients} in here would force a Kafka client into every consumer of the library —
 * including services that already have one, at a version they did not choose. So the library depends
 * on the ANSWER it needs ("did the broker confirm?") rather than on the machinery that produces it.
 *
 * <h2>Contract</h2>
 * Both halves are load-bearing; an implementation that breaks either one silently downgrades the
 * guarantee the library advertises.
 *
 * <ol>
 *   <li><strong>Do not wait for the acknowledgement on the calling thread.</strong> Hand the record
 *       to the producer and return; report the outcome from the producer's completion callback. The
 *       first attempt runs on the thread that announced — a request thread, or worse a broker
 *       LISTENER thread driving a cascade of N announcements. A blocking attempt costs that thread
 *       N &times; (broker patience) with the broker down, which overruns the consumer's
 *       {@code max.poll.interval.ms} and turns a broker outage into a consumer-group rebalance:
 *       a delivery problem promoted to an availability problem. When a caller does need to wait, it
 *       is {@link OutboxPublisher#publishAndWait} that waits — on a thread where waiting is free —
 *       never the implementation of this method.
 *       <p>"Does not wait" is precise, not absolute: an asynchronous producer still blocks the
 *       caller until the record is accepted into its buffer (for Kafka: the metadata fetch and
 *       buffer space, up to {@code max.block.ms}). That dial is the SERVICE'S to pin explicitly —
 *       Kafka's own 60s default makes this paragraph false. Pin it, and pin it in a test.</li>
 *   <li><strong>Call the outcome exactly once, and call {@code confirmed()} only when the broker
 *       ACKNOWLEDGED.</strong> Not when the record was serialised, not when it was buffered, not
 *       when {@code send} returned — the mark that follows a confirmation is what lets the library
 *       stop caring about the event, so an optimistic confirmation is a lost event with a green
 *       log. A synchronous refusal (producer closed, buffer full, serialisation error) is a
 *       {@code failed(cause)}. Throwing instead of calling {@code failed} is tolerated — the
 *       library treats the throw as a failure — but then do not also call the outcome.</li>
 * </ol>
 *
 * <p>The implementation should also stamp any transport header the estate expects (a correlation
 * header) from {@link OutboxEvent#cid()} — the STORED value, never a thread-local read at send
 * time. A republication happens hours later, on a scheduler thread whose context is empty; reading
 * the ambient correlation id there yields nothing, or worse, someone else's.
 */
@FunctionalInterface
public interface Dispatch {

    void send(OutboxEvent event, DispatchOutcome outcome);
}
