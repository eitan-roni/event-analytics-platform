import time
from collections.abc import Callable
from typing import Protocol, TypeVar

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from app.core.exceptions import CacheError

# Short timeouts so a slow or unreachable Redis cannot stall a request for
# long; callers fall back to the database on CacheError.
CONNECT_TIMEOUT_SECONDS = 0.2
READ_TIMEOUT_SECONDS = 0.2
# After a failed cache call, skip Redis for this long. Some failures take far
# longer than the socket timeouts (e.g. DNS resolution of a host whose
# container is gone), so during an outage at most one request per window
# pays that cost and the rest go straight to the database.
FAILURE_COOLDOWN_SECONDS = 10.0

T = TypeVar("T")


class Cache(Protocol):
    def get(self, key: str) -> str | None:
        """Return the cached value, or None if absent. Raises CacheError."""
        ...

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        """Store a value that expires after ttl_seconds. Raises CacheError."""
        ...


class RedisCache:
    def __init__(
        self,
        url: str,
        failure_cooldown_seconds: float = FAILURE_COOLDOWN_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._failure_cooldown_seconds = failure_cooldown_seconds
        self._clock = clock
        # Concurrent requests may race on this; at worst a few extra calls
        # reach Redis around a failure, which is harmless.
        self._skip_until = 0.0
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
        return self._call("read", lambda: self._client.get(key))  # type: ignore[return-value]

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        self._call("write", lambda: self._client.set(key, value, ex=ttl_seconds))

    def _call(self, operation: str, command: Callable[[], T]) -> T:
        if self._clock() < self._skip_until:
            raise CacheError(f"Cache {operation} skipped: Redis failed recently")
        try:
            return command()
        except redis.RedisError as exc:
            self._skip_until = self._clock() + self._failure_cooldown_seconds
            raise CacheError(f"Cache {operation} failed: {exc}") from exc

    def ping(self) -> None:
        """Readiness probe: raise CacheError if Redis does not answer.

        Always contacts Redis, even while get/set are skipping it.
        """
        try:
            self._client.ping()
        except redis.RedisError as exc:
            raise CacheError(f"Cache ping failed: {exc}") from exc

    def close(self) -> None:
        self._client.close()
