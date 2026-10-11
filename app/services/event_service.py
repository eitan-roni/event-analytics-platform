import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from pydantic import ValidationError

from app.cache.cache import Cache
from app.core.config import STATS_CACHE_TTL_SECONDS
from app.core.exceptions import CacheError
from app.models.event import EventORM
from app.repositories.event_repository import EventRepository
from app.schemas.event import (
    EventCreate,
    EventFilters,
    EventListResponse,
    EventRead,
    EventStats,
)

logger = logging.getLogger(__name__)

STATS_CACHE_KEY_PREFIX = "stats:v1:"


@dataclass(frozen=True)
class StatsResult:
    stats: EventStats
    cache_hit: bool


def _normalize_time(value: datetime) -> str:
    # The same instant in different offsets shares one key. Naive values are
    # kept as given: their meaning depends on the database session timezone.
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc)
    return value.isoformat()


def stats_cache_key(filters: EventFilters) -> str:
    """Deterministic cache key for the stats of the given filters.

    The filters are serialized canonically (sorted keys, unset filters
    omitted, aware datetimes in UTC) and hashed, since the values come from
    query strings and can be arbitrarily long.
    """
    normalized: dict[str, str] = {}
    for name in ("source", "event_type", "severity"):
        value = getattr(filters, name)
        if value is not None:
            normalized[name] = value
    for name in ("start_time", "end_time"):
        value = getattr(filters, name)
        if value is not None:
            normalized[name] = _normalize_time(value)
    canonical = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return STATS_CACHE_KEY_PREFIX + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class EventService:
    def __init__(
        self,
        repository: EventRepository,
        stats_cache: Cache | None = None,
        stats_cache_ttl_seconds: int = STATS_CACHE_TTL_SECONDS,
    ) -> None:
        self._repository = repository
        self._stats_cache = stats_cache
        self._stats_cache_ttl_seconds = stats_cache_ttl_seconds

    def store(self, event: EventCreate) -> bool:
        """Persist an event; returns False if its event_id was already stored."""
        event_orm = EventORM(
            event_id=event.event_id,
            source=event.source,
            event_type=event.event_type,
            severity=event.severity.value,
            timestamp=event.timestamp,
            value=event.value,
        )
        return self._repository.insert_if_absent(event_orm)

    def list_events(self, filters: EventFilters, limit: int, offset: int) -> EventListResponse:
        items = self._repository.list_events(filters, limit, offset)
        total = self._repository.count(filters)
        return EventListResponse(
            items=[EventRead.model_validate(item) for item in items],
            total=total,
        )

    def get_stats(self, filters: EventFilters) -> StatsResult:
        """Stats for the filters, served from the cache when possible.

        The cache fails open: any cache error is logged and the stats are
        computed from the database instead. Entries expire by TTL only, so
        cached stats can be up to one TTL stale.
        """
        cache = self._stats_cache
        if cache is None:
            return StatsResult(self._compute_stats(filters), cache_hit=False)

        key = stats_cache_key(filters)
        cached = self._read_cached_stats(cache, key)
        if cached is not None:
            return StatsResult(cached, cache_hit=True)

        stats = self._compute_stats(filters)
        try:
            cache.set(key, stats.model_dump_json(), self._stats_cache_ttl_seconds)
        except CacheError as exc:
            logger.warning("Stats cache write failed; result not cached: %s", exc)
        return StatsResult(stats, cache_hit=False)

    @staticmethod
    def _read_cached_stats(cache: Cache, key: str) -> EventStats | None:
        try:
            raw = cache.get(key)
        except CacheError as exc:
            logger.warning("Stats cache read failed; using the database: %s", exc)
            return None
        if raw is None:
            return None
        try:
            return EventStats.model_validate_json(raw)
        except ValidationError:
            logger.warning("Ignoring unreadable stats cache entry %s", key)
            return None

    def _compute_stats(self, filters: EventFilters) -> EventStats:
        total, avg_value, min_value, max_value, by_severity, by_event_type = (
            self._repository.get_stats(filters)
        )
        return EventStats(
            total=total,
            by_severity=by_severity,
            by_event_type=by_event_type,
            avg_value=float(avg_value) if avg_value is not None else None,
            min_value=float(min_value) if min_value is not None else None,
            max_value=float(max_value) if max_value is not None else None,
        )
