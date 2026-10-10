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

Kafka, Redis, Docker image, and monitoring are not implemented yet. See
[docs/ROADMAP.md](docs/ROADMAP.md) for the full plan.

## Project Structure

```
app/     - application source code
tests/   - test suite
```
