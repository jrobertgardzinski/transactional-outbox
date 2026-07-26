# transactional-outbox

An event whose transaction committed **will** eventually reach the broker.

Framework-free core (`com.jrobertgardzinski:transactional-outbox`) plus a Spring adapter in the
sibling module `infrastructure-spring-outbox` — the same split as `adjustable-clock` +
`infrastructure-micronaut-clock`, and for the same reason: consumers sit on Spring Boot, Micronaut
and Quarkus, so the core may not drag any framework in.

## What it depends on

`java.sql` (JDK) and `slf4j-api`. **Not** on a Kafka client — sending is the `Dispatch` interface the
service implements with whatever producer it already holds. **Not** on Jackson — the payload is an
opaque `String`, because the library's promise is delivery, not meaning.

## One pass, three collaborators

```
business transaction ──► TransactionalOutbox.append(Connection, OutboxEvent)   row, not sent
       commit         ──► OutboxPublisher.publishWithoutWaiting(event)         best effort
                                └─ broker confirmed ──► row marked published
every 15s             ──► OutboxRepublisher.runOnce()                          the guarantee
                                ├─ reap delivered rows past retention
                                └─ re-send unconfirmed rows past minAge, wait, then mark
```

The first attempt is best effort and never blocks the announcing thread. The republisher is the
guarantee and blocks on purpose — nobody is waiting on a scheduler thread.

## What the service brings

**1. The table.** The library ships the shape, not the migration: every service owns its own Flyway
history, and a library that claimed a version number in someone else's sequence would be wrong the
first time two services shared a database. Paste `OutboxTable.named("…").ddl()` into your own
`V<n>__…sql`. The library's tests create their fixtures by executing exactly that string, so the
documented shape cannot drift from the shape the queries expect.

**2. A `Dispatch`.** Two contract points, both load-bearing — read the interface's javadoc:
do not wait for the acknowledgement on the calling thread, and call `confirmed()` only when the
**broker acknowledged**.

**3. A scheduler** to call `OutboxRepublisher.runOnce()`. Spring services get
`ScheduledOutboxRepublisher` from the adapter module.

**4. Its own producer clocks.** The library cannot see a producer property, and property (c) — "the
announcing thread does not wait" — is false no matter what this code does if `max.block.ms` is left
at Kafka's 60s default. Pin `max.block.ms`, `delivery.timeout.ms` and `request.timeout.ms`
explicitly, reconcile them with the consumer's `max.poll.interval.ms`, and pin them in a test.

**5. The event id inside its payload.** `OutboxEvent.id()` is the row key; the service embeds it in
the payload so a redelivery is a recognisable duplicate rather than a second event. The library
cannot do it — it does not know the payload's shape.

## Spring, in full

```java
@Bean
OutboxTable memeOutboxTable() {
    return OutboxTable.named("meme_events_outbox");
}

@Bean
@ConditionalOnProperty(name = "memes.kafka-enabled", havingValue = "true")
SpringOutbox memeOutbox(DataSource dataSource, OutboxTable table, Clock clock, Dispatch dispatch) {
    return new SpringOutbox(dataSource, table, clock, dispatch);
}

@Bean
@ConditionalOnProperty(name = "memes.kafka-enabled", havingValue = "true")
ScheduledOutboxRepublisher memeOutboxRepublisher(SpringOutbox outbox,
        @Value("${memes.outbox.retention-hours:24}") long retentionHours) {
    return new ScheduledOutboxRepublisher(new OutboxRepublisher(outbox.outbox(), outbox.publisher(),
            RepublisherSettings.defaults(
                    OutboxDials.retentionHours("memes.outbox.retention-hours", retentionHours))));
}
```

Then the announcement is one call, inside the transaction that makes it true:

```java
String eventId = OutboxEvent.newId();
outbox.announce(new OutboxEvent(eventId, TOPIC, "MEME_DELETED", memeId,
        MDC.get(CorrelationIdFilter.MDC_KEY),
        "{\"type\":\"MEME_DELETED\",\"memeId\":\"" + memeId + "\",\"eventId\":\"" + eventId + "\"}"));
```

`@EnableScheduling` stays in the service, next to the bean that needs it: a library has no business
switching on a framework subsystem in its host.

## Where each guarantee lives

The full table — the eight properties this library was extracted to preserve, which of them the
library enforces and which remain the service's, with the reasoning — is in the package javadoc
(`com.jrobertgardzinski.outbox`, `package-info.java`).

## Who uses it (portal, round 10)

- **microservice-memes**, `meme_events_outbox` — the implementation this library was extracted FROM.
  It migrated first on purpose: it carries the eight properties hardened over three rounds, each with
  its own test, so a green suite there was the evidence that the abstraction ate none of them. Its own
  `MemeEventsOutbox` and `MemeEventsOutboxRepublisher` are gone; `KafkaMemeEvents`,
  `KafkaMemeDispatch` and `MemeOutboxConfig` are all that stayed behind. No migration was needed —
  the table V5/V6 built matches `OutboxTable.ddl()` column for column, index name included.
- **microservice-comments**, `comment_events_outbox` (`V4`) — a PROMOTION rather than a move. It used
  to publish `COMMENTS_DELETED` after the commit with a callback that logged failures, and argued in
  its javadoc that an outbox was not worth its price at the cascade's stakes. The argument was about
  price, and this library is what changed it: what the service pays now is one migration and one
  configuration class. The cascade hop's transaction moved into `MemesEventsListener` so the row is
  written with the thread delete, and the envelope id became the row id — because a republication path
  now exists and a duplicate has to be recognisable.

Both services keep their own test that an UNCONFIRMED send leaves the row unpublished. That is the one
property the library must trust its host on (see `Dispatch`), so it is the one worth re-proving where
the real producer is.

## What this is deliberately not

Not every "do not lose a message" problem is an outbox:

- **A synchronous confirmation sent before a consumer commits its offset** is a different promise
  ("the sender knows it was accepted"), not a weaker version of this one. It has no row and needs none.
- **A flag on a saga's own state row** (`outcome_announced`) is already transactional with the state
  it guards and already deduplicates against the saga's identity. A generic table beside it would add
  a second source of truth about the same fact.

## Build

```bash
../mvnw -pl transactional-outbox,infrastructure-spring-outbox test
```

JDK 25. The tests need no service, no broker and no container — H2 in PostgreSQL mode and a fake
`Dispatch` that can hold an acknowledgement open, which is how the "does not wait for the broker"
property is pinned by a measurement rather than by inspection.
