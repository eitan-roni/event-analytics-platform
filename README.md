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

Redis, Docker image, and monitoring are not implemented yet. See
[docs/ROADMAP.md](docs/ROADMAP.md) for the full plan.

## Architecture

```
client -> API (POST /v1/events) -> Kafka topic "events" -> consumer -> PostgreSQL
                                                                        ^
client -> API (GET /v1/events, GET /v1/events/stats) -------------------+
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
  - Malformed or invalid messages (same validation as the API) are logged
    and skipped, and their offset is committed.
  - Messages the database rejects as such (`DataError`, `IntegrityError`)
    would fail on every retry, so they are logged at error level with topic,
    partition and offset, skipped, and their offset is committed. A
    dead-letter topic for these is planned (see the roadmap).
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
partitions (topic auto-creation is disabled on the broker). The broker has no
volume, so its data is discarded by `docker compose down`; the topic is
recreated on the next `up`.

Configuration (environment variables, see `.env.example`):

| Variable                  | Default                                                               |
|---------------------------|-----------------------------------------------------------------------|
| `DATABASE_URL`            | `postgresql+psycopg://postgres:postgres@localhost:5432/event_analytics` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`                                                      |
| `KAFKA_EVENTS_TOPIC`      | `events`                                                              |
| `KAFKA_CONSUMER_GROUP`    | `event-consumer`                                                      |

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

### Integration test (Kafka)

One end-to-end test (POST -> Kafka -> consumer logic -> row in PostgreSQL) is
marked `integration` and excluded from the default run. With
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
