from typing import Protocol

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from app.core.exceptions import CacheError

# Short timeouts so a slow or unreachable Redis cannot stall a request for
# long; callers fall back to the database on CacheError.
CONNECT_TIMEOUT_SECONDS = 0.2
READ_TIMEOUT_SECONDS = 0.2


class Cache(Protocol):
    def get(self, key: str) -> str | None:
        """Return the cached value, or None if absent. Raises CacheError."""
        ...

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        """Store a value that expires after ttl_seconds. Raises CacheError."""
        ...


class RedisCache:
    def __init__(self, url: str) -> None:
        # from_url does not connect yet, so the app starts even if Redis is
        # down. redis-py retries with backoff by default, which would multiply
        # the time a dead Redis adds to each request; retries are disabled.
        self._client = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=CONNECT_TIMEOUT_SECONDS,
            socket_timeout=READ_TIMEOUT_SECONDS,
            retry=Retry(NoBackoff(), 0),
        )

    def get(self, key: str) -> str | None:
        try:
            return self._client.get(key)  # type: ignore[return-value]
        except redis.RedisError as exc:
            raise CacheError(f"Cache read failed: {exc}") from exc

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        try:
            self._client.set(key, value, ex=ttl_seconds)
        except redis.RedisError as exc:
            raise CacheError(f"Cache write failed: {exc}") from exc

    def close(self) -> None:
        self._client.close()
