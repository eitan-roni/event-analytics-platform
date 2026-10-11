# Roadmap

## Done
- v0.1 - Event ingestion API (POST /v1/events, validation, health check)
- v0.2 - PostgreSQL persistence (SQLAlchemy, service/repository layers,
  separate test database)
- v0.3 - Query and stats API; duplicate event_id handling
- v0.4 - Kafka ingestion: the API publishes to a topic, a separate
  consumer persists events
- v0.5 - Dead-letter topic for unprocessable messages
- v0.6 - Redis cache for the stats endpoint (TTL expiry, fail-open)
- v0.7 - App Docker image (API and consumer), Alembic migrations, pinned
  dependencies
- v0.8 - Prometheus metrics and readiness checks

## Planned
- Nothing planned yet.

## Working agreement
- One version per commit, message "Implement v0.X <summary>", pushed to main.
- Every version ships with tests and a README update.
