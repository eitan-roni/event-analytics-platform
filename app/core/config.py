import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@localhost:5432/event_analytics",
)

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@localhost:5432/event_analytics_test",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

KAFKA_EVENTS_TOPIC = os.getenv("KAFKA_EVENTS_TOPIC", "events")

KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "event-consumer")

KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "events.dlq")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

STATS_CACHE_TTL_SECONDS = int(os.getenv("STATS_CACHE_TTL_SECONDS", "30"))
if STATS_CACHE_TTL_SECONDS < 1:
    raise ValueError("STATS_CACHE_TTL_SECONDS must be at least 1")

CONSUMER_METRICS_PORT = int(os.getenv("CONSUMER_METRICS_PORT", "9101"))
