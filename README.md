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

Kafka, Redis, Docker image, and monitoring are not implemented yet. See
[docs/ROADMAP.md](docs/ROADMAP.md) for the full plan.

## Project Structure

```
app/     - application source code
tests/   - test suite
```

## Run

Set `DATABASE_URL` (defaults to
`postgresql+psycopg://postgres:postgres@localhost:5432/event_analytics`) and
start the API:

```
uvicorn app.main:app --reload
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
always tests against the current models.

### Recreating the dev database after an index/schema change

`Base.metadata.create_all` only creates tables that don't exist yet — it
won't add columns or indexes to a table that's already there. v0.3 added
indexes on `events.timestamp`, `events.source`, and `events.event_type`; if
your local `events` table predates v0.3, drop it and let the app recreate it:

```
psql "$DATABASE_URL" -c "DROP TABLE IF EXISTS events;"
```

Then restart the app. Proper migrations (Alembic) are planned for v0.6.
