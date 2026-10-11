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
- **v0.6 — implemented.** Redis cache for `GET /v1/events/stats` (TTL
  expiry, fail-open).
- **v0.7 — implemented.** One Docker image for the API and the consumer,
  Alembic migrations, pinned dependencies.

Metrics and readiness checks are not implemented yet. See
[docs/ROADMAP.md](docs/ROADMAP.md) for the full plan.

## Architecture

```
client -> API (POST /v1/events) -> Kafka topic "events" -> consumer -> PostgreSQL
                                                              |          ^
                                       rejected messages      v          |
                                             Kafka topic "events.dlq"    |
                                                                         |
client -> API (GET /v1/events) ------------------------------------------+
                                                                         |
client -> API (GET /v1/events/stats) -> Redis cache --- on miss ---------+
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

### Stats cache

`GET /v1/events/stats` is cached in Redis; `GET /v1/events` is not.

- **Key:** `stats:v1:` followed by a SHA-256 of the normalized filters
  (unset filters omitted, timezone-aware times converted to UTC), so
  equivalent filters, such as the same instant written with different UTC
  offsets, share one entry. The value is the response JSON. Bump the `v1`
  prefix when the response format changes, so old entries are ignored.
- **Expiry:** by TTL only (`STATS_CACHE_TTL_SECONDS`, default 30). Nothing is
  invalidated when events are written, so stats can be up to one TTL stale.
  That is acceptable because ingestion is asynchronous anyway: a new event
  is not counted until the consumer has stored it.
- **Fail-open:** if Redis is unreachable, slow, or returns an error (or holds
  an unreadable entry), a warning is logged and the stats are computed from
  PostgreSQL. Redis never turns a working request into an error. Connect and
  read timeouts are 0.2 s and client retries are disabled, so a request
  waits at most about 0.2 s per cache operation (one read, and one write on
  a miss) when Redis is down or hung, until it recovers. During an outage
  every stats request logs a warning.
- **Observability:** the response carries `X-Cache: HIT` (served from Redis)
  or `X-Cache: MISS` (computed from PostgreSQL, including when Redis is
  unavailable).

## Project Structure

```
app/                 - application source code
  api/               - FastAPI routes
  services/          - business logic
  repositories/      - database access
  messaging/         - Kafka publishers (events, dead letters)
  cache/             - Redis cache
  consumer/          - Kafka consumer entry point (python -m app.consumer)
migrations/          - Alembic migrations (alembic.ini at the root)
tests/               - test suite
docker/              - Postgres init script (creates the test database)
Dockerfile           - one image for the API, the consumer and migrations
requirements.txt     - pinned runtime dependencies (installed in the image)
requirements-dev.txt - runtime + test dependencies
```

## Run

`docker compose up -d` starts the infrastructure only: PostgreSQL, Redis
(`localhost:6379`, no persistence: it only holds the cache) and a
single-node Kafka broker (KRaft mode, no ZooKeeper, reachable at
`localhost:9092`). The one-shot `kafka-init` service creates the `events`
topic with 3 partitions and the `events.dlq` dead-letter topic with 1
partition (topic auto-creation is disabled on the broker). The broker has no
volume, so its data is discarded by `docker compose down`; the topics are
recreated on the next `up`.

Neither the API nor the consumer creates tables: the schema is managed by
Alembic migrations, which must be applied first. There are two ways to run
the application.

### Option A: local processes against the compose infrastructure

For development (auto-reload, debugger):

```
docker compose up -d
pip install -r requirements-dev.txt
alembic upgrade head
uvicorn app.main:app --reload
```

Start the consumer in a second terminal:

```
python -m app.consumer
```

Configuration (environment variables, see `.env.example`; the defaults
point at the compose infrastructure on `localhost`):

| Variable                  | Default                                                                 |
|---------------------------|-------------------------------------------------------------------------|
| `DATABASE_URL`            | `postgresql+psycopg://postgres:postgres@localhost:5432/event_analytics` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`                                                        |
| `KAFKA_EVENTS_TOPIC`      | `events`                                                                |
| `KAFKA_DLQ_TOPIC`         | `events.dlq`                                                            |
| `KAFKA_CONSUMER_GROUP`    | `event-consumer`                                                        |
| `REDIS_URL`               | `redis://localhost:6379/0`                                              |
| `STATS_CACHE_TTL_SECONDS` | `30` (must be at least 1)                                               |

### Option B: everything in compose

```
docker compose --profile app up -d --build
```

This builds one image (`event-analytics-platform:latest`, Python 3.12 slim,
runtime dependencies only, running as a non-root user) and starts three more
services from it, configured to use the compose network names (`postgres`,
`redis`, `kafka:19092`):

- `migrate` runs `alembic upgrade head` once and exits;
- `api` serves on `http://localhost:8000` (the same port as a local uvicorn,
  so stop one before starting the other);
- `consumer` runs `python -m app.consumer` and is restarted by compose when
  it exits with an error (database or dead-letter failure).

`api` and `consumer` start only after the infrastructure is healthy, the
topics exist and `migrate` has completed successfully. Follow the logs with
`docker compose --profile app logs -f api consumer`; stop everything with
`docker compose --profile app down`.

## Database migrations

The schema is defined by the SQLAlchemy models and created by Alembic
migrations in `migrations/versions/`. `migrations/env.py` reads
`DATABASE_URL` from `app/core/config.py`.

After changing a model, generate a migration, review it, and apply it:

```
alembic revision --autogenerate -m "describe the change"
alembic upgrade head
```

Autogenerate compares the models with the database at `DATABASE_URL`, so
that database must be at the current head first. Always read the generated
file; autogenerate does not detect everything (e.g. renames). If you forget
the migration, `tests/test_migrations.py` fails: it applies all migrations
to an empty schema in the test database and compares the result with the
models.

### Moving an existing dev database to Alembic

Before v0.7 the app created the `events` table itself (`create_all`), so an
existing dev database has the table but no record of migrations, and
`alembic upgrade head` fails with "relation already exists". Either:

- **Recreate the database** (deletes all data, including the test database,
  which the init script recreates):

  ```
  docker compose down -v
  docker compose up -d
  alembic upgrade head
  ```

- **Keep the data** and mark the existing table as migrated:

  ```
  alembic stamp head
  ```

  Only do this if the table matches the migration, i.e. it was created by
  v0.3 or later. Tables from v0.1/v0.2 lack the indexes on `timestamp`,
  `source` and `event_type` (`\d events` in psql shows them); for those,
  recreate the database instead.

## Tests

Install the dev requirements (`pip install -r requirements-dev.txt`) and set
`TEST_DATABASE_URL` (defaults to
`postgresql+psycopg://postgres:postgres@localhost:5432/event_analytics_test`)
to a database separate from `DATABASE_URL` — the test suite refuses to run
otherwise — then:

```
pytest
```

The test suite creates and drops the schema from the models around the whole
run, so it always tests against the current models; the migrations are
checked against the models separately (see above). The default run needs
only PostgreSQL: the API tests replace the Kafka publisher and the cache with
in-memory fakes, and the consumer's message handling is tested without a
broker.

### Integration tests (Kafka, Redis)

Three tests are marked `integration` and excluded from the default run:

- POST -> Kafka -> consumer logic -> row in PostgreSQL
- an invalid message on `events` -> consumer logic -> `events.dlq` with its
  headers
- the stats endpoint against the real Redis: MISS, then HIT, and the key's
  TTL is set

With `docker compose up -d` running:

```
pytest -m integration
```
