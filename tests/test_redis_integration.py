"""Stats cache against the real Redis from docker-compose.

Excluded from the default run; use: pytest -m integration
"""

import uuid
from collections.abc import Generator

import pytest
import redis
from fastapi.testclient import TestClient

from app.api.events import get_stats_cache
from app.cache.cache import RedisCache
from app.core.config import REDIS_URL, STATS_CACHE_TTL_SECONDS
from app.main import app
from app.schemas.event import EventFilters
from app.services.event_service import stats_cache_key

pytestmark = pytest.mark.integration


@pytest.fixture
def redis_cache() -> Generator[RedisCache, None, None]:
    cache = RedisCache(REDIS_URL)
    try:
        yield cache
    finally:
        cache.close()


def test_stats_are_cached_in_redis_with_ttl(
    client: TestClient, redis_cache: RedisCache
) -> None:
    app.dependency_overrides[get_stats_cache] = lambda: redis_cache
    # A unique filter, so the entry cannot collide with dev data or earlier runs.
    source = f"it-redis-{uuid.uuid4()}"
    key = stats_cache_key(EventFilters(source=source))
    inspector = redis.Redis.from_url(REDIS_URL)
    try:
        first = client.get("/v1/events/stats", params={"source": source})
        second = client.get("/v1/events/stats", params={"source": source})

        assert first.status_code == 200
        assert first.headers["X-Cache"] == "MISS"
        assert second.status_code == 200
        assert second.headers["X-Cache"] == "HIT"
        assert second.json() == first.json()
        assert 0 < inspector.ttl(key) <= STATS_CACHE_TTL_SECONDS
    finally:
        inspector.delete(key)
        inspector.close()
