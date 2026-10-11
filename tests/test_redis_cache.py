from unittest.mock import MagicMock

import pytest
import redis

from app.cache.cache import RedisCache
from app.core.exceptions import CacheError

COOLDOWN = 10.0


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> _Clock:
    return _Clock()


@pytest.fixture
def client() -> MagicMock:
    return MagicMock(spec=redis.Redis)


@pytest.fixture
def cache(client: MagicMock, clock: _Clock) -> RedisCache:
    # The URL is never contacted: the Redis client is replaced by a mock.
    cache = RedisCache("redis://unused:6379/0", failure_cooldown_seconds=COOLDOWN, clock=clock)
    cache._client = client
    return cache


def test_calls_pass_through_while_redis_works(cache: RedisCache, client: MagicMock) -> None:
    client.get.return_value = "value"

    assert cache.get("k") == "value"
    cache.set("k", "v", 30)

    client.set.assert_called_once_with("k", "v", ex=30)


def test_redis_is_skipped_for_the_cooldown_after_a_failure(
    cache: RedisCache, client: MagicMock, clock: _Clock
) -> None:
    client.get.side_effect = redis.ConnectionError("unreachable")

    with pytest.raises(CacheError, match="read failed"):
        cache.get("k")
    clock.now += COOLDOWN - 1
    with pytest.raises(CacheError, match="skipped"):
        cache.get("k")
    with pytest.raises(CacheError, match="skipped"):
        cache.set("k", "v", 30)

    assert client.get.call_count == 1
    client.set.assert_not_called()


def test_redis_is_tried_again_after_the_cooldown(
    cache: RedisCache, client: MagicMock, clock: _Clock
) -> None:
    client.get.side_effect = [redis.TimeoutError("slow"), "value"]
    with pytest.raises(CacheError):
        cache.get("k")

    clock.now += COOLDOWN

    assert cache.get("k") == "value"
    assert client.get.call_count == 2


def test_ping_always_contacts_redis(cache: RedisCache, client: MagicMock) -> None:
    client.get.side_effect = redis.ConnectionError("unreachable")
    with pytest.raises(CacheError):
        cache.get("k")

    cache.ping()  # readiness must report the real state, not the skip window

    client.ping.assert_called_once()
