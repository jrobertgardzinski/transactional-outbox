/**
 * The transactional outbox, framework-free: <em>an event whose transaction committed WILL eventually
 * reach the broker</em>.
 *
 * <h2>Shape</h2>
 * <pre>
 *   business transaction ──► TransactionalOutbox.append(Connection, OutboxEvent)   (row, not sent)
 *          commit         ──► OutboxPublisher.publishWithoutWaiting(event)          (best effort)
 *                                    └─ broker confirmed ──► row marked published
 *   every N seconds       ──► OutboxRepublisher.runOnce()                           (the guarantee)
 *                                    ├─ reap delivered rows past retention
 *                                    └─ re-send unconfirmed rows past minAge, wait, then mark
 * </pre>
 * The service supplies three things: the table (its own Flyway migration, shape from
 * {@link com.jrobertgardzinski.outbox.OutboxTable#ddl()}), a {@link com.jrobertgardzinski.outbox.Dispatch}
 * over whatever producer it already has, and a scheduler to call {@code runOnce}. Framework wiring
 * for Spring lives in the sibling module {@code infrastructure-spring-outbox}.
 *
 * <h2>Who guarantees what</h2>
 * The properties this library was extracted to preserve, and where each of them lives. The split is
 * not arbitrary: everything that can be enforced without knowing the framework, the broker or the
 * payload format is enforced HERE; the rest is documented as the service's, because a library that
 * pretended to own it would only be able to hope.
 *
 * <table class="striped">
 *   <caption>Guarantee ownership</caption>
 *   <thead><tr><th>Property</th><th>Owner</th><th>Where</th></tr></thead>
 *   <tbody>
 *   <tr><td>(a) the row is written in the SAME transaction as the change it announces — a rollback
 *           leaves no row and therefore no event</td>
 *       <td>LIBRARY (mechanism) + SERVICE (calling it inside the transaction)</td>
 *       <td>{@link com.jrobertgardzinski.outbox.TransactionalOutbox#append}; the Spring adapter's
 *           {@code SpringOutbox.append} reaches the transaction-bound connection so the service
 *           cannot get this wrong by accident</td></tr>
 *   <tr><td>(b) delivered-first: {@code published_at} is stamped only after the broker CONFIRMED, never
 *           after a flush or a buffered send</td>
 *       <td>LIBRARY (the mark) + SERVICE (an honest {@code confirmed()})</td>
 *       <td>{@link com.jrobertgardzinski.outbox.OutboxPublisher};
 *           {@link com.jrobertgardzinski.outbox.Dispatch} contract point 2</td></tr>
 *   <tr><td>(c) the first attempt does not block the announcing thread</td>
 *       <td>LIBRARY (never waits) + SERVICE (an asynchronous send)</td>
 *       <td>{@link com.jrobertgardzinski.outbox.OutboxPublisher#publishWithoutWaiting};
 *           {@link com.jrobertgardzinski.outbox.Dispatch} contract point 1</td></tr>
 *   <tr><td>(d) the republisher re-sends unmarked rows older than a threshold and marks them after
 *           confirmation</td>
 *       <td>LIBRARY</td>
 *       <td>{@link com.jrobertgardzinski.outbox.OutboxRepublisher#runOnce()} +
 *           {@link com.jrobertgardzinski.outbox.OutboxPublisher#publishAndWait}</td></tr>
 *   <tr><td>(e) batched retention of delivered rows, capped per pass, and run BEFORE the re-send</td>
 *       <td>LIBRARY</td>
 *       <td>{@link com.jrobertgardzinski.outbox.TransactionalOutbox#deletePublishedOlderThan} and the
 *           order inside {@code runOnce}</td></tr>
 *   <tr><td>(f) an undeliverable event backs off instead of holding the head of the queue, and is
 *           eventually left alone rather than retried for ever</td>
 *       <td>LIBRARY</td>
 *       <td>{@link com.jrobertgardzinski.outbox.TransactionalOutbox#recordFailedAttempt} and the
 *           {@code attempts} / {@code next_attempt_at} columns. Added 2026-07-28: without them a
 *           payload past the broker's message limit was retried every interval for ever AND, since
 *           the poll is oldest-first, sat at the head of the batch — enough of those and no newer
 *           event was ever selected again</td></tr>
 *   <tr><td><strong>NOT guaranteed:</strong> a single relay. Two instances both poll and both send —
 *           the poll has no {@code FOR UPDATE SKIP LOCKED} and no lease, deliberately, because
 *           holding a database transaction across broker I/O is worse than a duplicate. Duplicates
 *           scale with the instance count, so a deployment that runs more than one replica of a
 *           service with an outbox must accept that, and a rolling update is itself a brief
 *           two-instance window (hence {@code strategy: Recreate} on both deployments)</td>
 *       <td>SERVICE — in its deployment</td>
 *       <td>k8s/base/memes.yaml, k8s/base/comments.yaml</td></tr>
 *   <tr><td>(g) the dials are range-checked, naming the property and echoing the value</td>
 *       <td>LIBRARY (the checks and the wording) + SERVICE (the property's name)</td>
 *       <td>{@link com.jrobertgardzinski.outbox.OutboxDials},
 *           {@link com.jrobertgardzinski.outbox.RepublisherSettings}</td></tr>
 *   <tr><td>(h) payload-width canary; the event id is the row id; the correlation id lives IN the row
 *           and is never read from a thread-local at send time</td>
 *       <td>LIBRARY (canary, id, stored cid) + SERVICE (embedding the id in its payload)</td>
 *       <td>{@link com.jrobertgardzinski.outbox.TransactionalOutbox#EXPECTED_MAX_PAYLOAD_CHARS},
 *           {@link com.jrobertgardzinski.outbox.OutboxEvent}</td></tr>
 *   <tr><td>(i) the producer's own clocks ({@code max.block.ms}, {@code delivery.timeout.ms},
 *           {@code request.timeout.ms}) are pinned explicitly and reconciled with the consumer's
 *           {@code max.poll.interval.ms}</td>
 *       <td>SERVICE — unavoidably</td>
 *       <td>this library has no broker client and cannot see, let alone set, a producer property.
 *           What it does instead is state the dependency where an implementer meets it
 *           ({@link com.jrobertgardzinski.outbox.Dispatch} contract point 1): property (c) is a
 *           promise about a thread, and an unpinned {@code max.block.ms} (Kafka's default is 60s)
 *           makes it false no matter what this code does. Pin the dials AND pin them in a test.</td></tr>
 *   </tbody>
 * </table>
 *
 * <h2>What this library deliberately is not</h2>
 * Not every "do not lose a message" problem is an outbox, and forcing the ones that are not into
 * this shape would be fitting reality to an abstraction:
 * <ul>
 *   <li><strong>A synchronous confirmation before a consumer commits its offset</strong> is a
 *       different promise ("the sender knows it was accepted"), not a weaker version of this one. It
 *       has no row and needs none.</li>
 *   <li><strong>A flag on a saga's own state row</strong> ({@code outcome_announced}) is already
 *       transactional with the state it guards and already deduplicates against the saga's identity.
 *       A generic table next to it would add a second source of truth about the same fact.</li>
 * </ul>
 */
package com.jrobertgardzinski.outbox;
