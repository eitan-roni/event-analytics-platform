# Event Analytics Platform

An event ingestion and analytics platform.

## Goal

Ingest, process, and analyze event data at scale, exposing the results through
an API for downstream consumers.

## Planned Stack

- **FastAPI** — API layer for event ingestion and analytics queries
- **PostgreSQL** — durable storage for processed events and aggregates
- **Kafka** — event streaming / ingestion pipeline
- **Redis** — caching and fast lookups
- **Docker** — containerized local development and deployment
- **Monitoring tools** — observability into system health and data flow

## Status

- **v0.1 — implemented.** Event ingestion API (`POST /v1/events`, validation,
  health check).
- **v0.2 — implemented.** PostgreSQL persistence (SQLAlchemy, service and
  repository layers, separate test database).
- **v0.3 — implemented.** Query and stats API (`GET /v1/events`,
  `GET /v1/events/stats`), duplicate `event_id` handling (409 Conflict).
- **v0.4 — implemented.** Kafka ingestion: `POST /v1/events` publishes to a
  topic and returns `202 Accepted`; a separate consumer persists events.
- **v0.5 — implemented.** Dead-letter topic: messages the consumer cannot
  process are preserved on `events.dlq` instead of being dropped.

Redis, Docker image, and monitoring are not implemented yet. See
[docs/ROADMAP.md](docs/ROADMAP.md) for the full plan.

## Architecture

```
client -> API (POST /v1/events) -> Kafka topic "events" -> consumer -> PostgreSQL
                                                              |          ^
                                       rejected messages      v          |
                                             Kafka topic "events.dlq"    |
                                                                         |
client -> API (GET /v1/events, GET /v1/events/stats) --------------------+
```

- The API validates the event and publishes it as JSON to the `events`
  topic, keyed by `source` (so events from one source
  keep their order within a partition). It waits for the broker's
  acknowledgement (`acks=all`, idempotent producer) and returns `202
  Accepted`, or `503` if the event could not be published in time. The API
  does not write to the database. Invalid input returns `422` and nothing is
  published; besides the field types, `event_id`, `source` and `event_type`
  must be non-empty and free of NUL characters (PostgreSQL text cannot store
  them), and `value` must be a finite number (no `NaN` / `Infinity`).
- The consumer (`python -m app.consumer`) reads the topic as consumer group
  `KAFKA_CONSUMER_GROUP` and inserts each event with
  `ON CONFLICT (event_id) DO NOTHING`. It commits the Kafka offset only after
  the database transaction is committed, so delivery is at-least-once and
  redelivered messages are harmless.
  - Messages that can never be processed are forwarded to the dead-letter
    topic (see [Dead-letter topic](#dead-letter-topic)) and their offset is
    committed: malformed or invalid payloads (same validation as the API),
    and valid events the database rejects as such (`DataError`,
    `IntegrityError`), which would fail on every retry.
  - If forwarding to the dead-letter topic fails, the consumer exits
    non-zero without committing the offset, so nothing is lost.
  - On any other database error (e.g. lost connection: `OperationalError`,
    `InterfaceError`) the consumer exits non-zero without committing the
    offset; the message is redelivered when it restarts.
  - If committing an offset fails with a non-fatal Kafka error (e.g. during
    a rebalance), the failure is logged and consumption continues; the
    message may be redelivered, which the idempotent insert makes harmless.
    A fatal Kafka error stops the consumer with a non-zero exit.
  - It stops cleanly on Ctrl+C / SIGTERM.
- Because persistence is asynchronous, a `202` means "queued", not
  "stored": the event shows up in the GET endpoints once the consumer has
  processed it. A duplicate `event_id` is accepted by the API and silently
  ignored by the consumer (the 409 response from v0.3 is gone).

### Dead-letter topic

Rejected messages go to `KAFKA_DLQ_TOPIC` (default `events.dlq`), keyed and
valued exactly as the original, so they can be inspected or replayed by
hand. Duplicates are normal behaviour and are not dead-lettered. The reason
is carried in headers:

| Header                   | Value                                           |
|--------------------------|-------------------------------------------------|
| `dlq.error.category`     | `invalid_payload` or `database_rejected`        |
| `dlq.error.detail`       | error description, truncated to 1000 characters |
| `dlq.original.topic`     | topic the message was read from                 |
| `dlq.original.partition` | its partition                                   |
| `dlq.original.offset`    | its offset                                      |

The consumer waits for the broker to acknowledge the dead-letter copy before
committing the original offset. A dead-letter copy whose acknowledgement
timed out may still be delivered, in which case the message is dead-lettered
again after the restart, so the topic can contain duplicates. There is no
automatic replay.

Read the topic with the console consumer inside the Kafka container:

```
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh   --bootstrap-server localhost:9092 --topic events.dlq --from-beginning   --property print.key=true --property print.headers=true
```

In Git Bash on Windows, prefix the command with `MSYS_NO_PATHCONV=1` so the
container path isn't rewritten into a Windows path.

## Project Structure

```
app/            - application source code
  api/          - FastAPI routes
  services/     - business logic
  repositories/ - database access
  messaging/    - Kafka publisher
  consumer/     - Kafka consumer entry point (python -m app.consumer)
tests/          - test suite
```

## Run

Start PostgreSQL and a single-node Kafka broker (KRaft mode, no ZooKeeper,
reachable at `localhost:9092`):

```
docker compose up -d
```

The one-shot `kafka-init` service creates the `events` topic with 3
partitions and the `events.dlq` dead-letter topic with 1 partition (topic
auto-creation is disabled on the broker). The broker has no volume, so its
data is discarded by `docker compose down`; the topics are recreated on the
next `up`.

Configuration (environment variables, see `.env.example`):

| Variable                  | Default                                                                 |
|---------------------------|-------------------------------------------------------------------------|
| `DATABASE_URL`            | `postgresql+psycopg://postgres:postgres@localhost:5432/event_analytics` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`                                                        |
| `KAFKA_EVENTS_TOPIC`      | `events`                                                                |
| `KAFKA_DLQ_TOPIC`         | `events.dlq`                                                            |
| `KAFKA_CONSUMER_GROUP`    | `event-consumer`                                                        |

Start the API:

```
uvicorn app.main:app --reload
```

Start the consumer in a second terminal:

```
python -m app.consumer
```

The app creates the `events` table on startup if it doesn't exist yet
(`Base.metadata.create_all`).

## Tests

Set `TEST_DATABASE_URL` (defaults to
`postgresql+psycopg://postgres:postgres@localhost:5432/event_analytics_test`)
to a database separate from `DATABASE_URL` — the test suite refuses to run
otherwise — then:

```
pytest
```

The test suite creates and drops the schema around the whole run, so it
always tests against the current models. The default run needs only
PostgreSQL: the API tests replace the Kafka publisher with an in-memory fake,
and the consumer's message handling is tested without a broker.

### Integration tests (Kafka)

Two end-to-end tests are marked `integration` and excluded from the default
run: POST -> Kafka -> consumer logic -> row in PostgreSQL, and an invalid
message on `events` -> consumer logic -> `events.dlq` with its headers. With
`docker compose up -d` running:

```
pytest -m integration
```

### Recreating the dev database after an index/schema change

`Base.metadata.create_all` only creates tables that don't exist yet — it
won't add columns or indexes to a table that's already there. v0.3 added
indexes on `events.timestamp`, `events.source`, and `events.event_type`; if
your local `events` table predates v0.3, drop it and let the app recreate it:

```
psql "$DATABASE_URL" -c "DROP TABLE IF EXISTS events;"
```

Then restart the app. Proper migrations (Alembic) are planned for v0.6.
