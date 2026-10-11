from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from app.models.event import EventORM
from app.repositories.event_repository import EventRepository
from app.schemas.event import EventFilters
from app.services.event_service import (
    STATS_CACHE_KEY_PREFIX,
    CacheStatus,
    EventService,
    stats_cache_key,
)
from tests.conftest import InMemoryCache

TTL_SECONDS = 30


def _seed(db_session: Session) -> None:
    db_session.add_all(
        [
            EventORM(
                event_id="evt-1",
                source="server-01",
                event_type="CPU_SPIKE",
                severity="LOW",
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                value=10.0,
            ),
            EventORM(
                event_id="evt-2",
                source="server-02",
                event_type="MEM_SPIKE",
                severity="HIGH",
                timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
                value=20.0,
            ),
        ]
    )
    db_session.commit()


@pytest.fixture
def repository(db_session: Session) -> MagicMock:
    # Spy: delegates to the real repository and records calls.
    _seed(db_session)
    return MagicMock(wraps=EventRepository(db_session))


@pytest.fixture
def service(repository: MagicMock, fake_cache: InMemoryCache) -> EventService:
    return EventService(repository, stats_cache=fake_cache, stats_cache_ttl_seconds=TTL_SECONDS)


def test_first_call_misses_and_second_call_hits(
    service: EventService, repository: MagicMock
) -> None:
    first = service.get_stats(EventFilters())
    second = service.get_stats(EventFilters())

    assert first.cache_status is CacheStatus.MISS
    assert second.cache_status is CacheStatus.HIT
    assert second.stats == first.stats
    assert first.stats.total == 2
    repository.get_stats.assert_called_once()


def test_value_is_stored_as_stats_json_with_configured_ttl(
    service: EventService, fake_cache: InMemoryCache
) -> None:
    result = service.get_stats(EventFilters(source="server-01"))

    assert fake_cache.set_calls == [
        (
            stats_cache_key(EventFilters(source="server-01")),
            result.stats.model_dump_json(),
            TTL_SECONDS,
        )
    ]


def test_different_filters_use_different_entries(
    service: EventService, repository: MagicMock
) -> None:
    all_events = service.get_stats(EventFilters())
    server_two = service.get_stats(EventFilters(source="server-02"))

    assert all_events.cache_hit is False
    assert server_two.cache_hit is False
    assert all_events.stats.total == 2
    assert server_two.stats.total == 1
    assert repository.get_stats.call_count == 2


@pytest.mark.parametrize(
    ("a", "b"),
    [
        pytest.param(EventFilters(), EventFilters(), id="no-filters"),
        pytest.param(
            EventFilters(source="s", severity="HIGH"),
            EventFilters(severity="HIGH", source="s"),
            id="same-values",
        ),
        pytest.param(
            EventFilters(start_time=datetime(2026, 1, 1, 2, tzinfo=timezone(timedelta(hours=2)))),
            EventFilters(start_time=datetime(2026, 1, 1, 0, tzinfo=timezone.utc)),
            id="same-instant-different-offset",
        ),
    ],
)
def test_equivalent_filters_share_a_key(a: EventFilters, b: EventFilters) -> None:
    assert stats_cache_key(a) == stats_cache_key(b)


@pytest.mark.parametrize(
    ("a", "b"),
    [
        pytest.param(EventFilters(), EventFilters(source="s"), id="filter-vs-none"),
        pytest.param(
            EventFilters(source="s"), EventFilters(event_type="s"), id="same-value-other-field"
        ),
        pytest.param(EventFilters(source=""), EventFilters(), id="empty-string-vs-none"),
        pytest.param(EventFilters(source="S"), EventFilters(source="s"), id="case-sensitive"),
        pytest.param(
            EventFilters(start_time=datetime(2026, 1, 1, tzinfo=timezone.utc)),
            EventFilters(end_time=datetime(2026, 1, 1, tzinfo=timezone.utc)),
            id="start-vs-end",
        ),
    ],
)
def test_different_filters_use_different_keys(a: EventFilters, b: EventFilters) -> None:
    assert stats_cache_key(a) != stats_cache_key(b)


def test_key_has_versioned_prefix() -> None:
    assert stats_cache_key(EventFilters()).startswith(STATS_CACHE_KEY_PREFIX)
    assert STATS_CACHE_KEY_PREFIX == "stats:v1:"


def test_entry_is_not_served_after_its_ttl(
    service: EventService, repository: MagicMock, fake_cache: InMemoryCache
) -> None:
    service.get_stats(EventFilters())

    fake_cache.now += TTL_SECONDS - 1
    assert service.get_stats(EventFilters()).cache_hit is True

    fake_cache.now += 1
    assert service.get_stats(EventFilters()).cache_hit is False
    assert repository.get_stats.call_count == 2


def test_cache_read_failure_falls_back_to_database(
    service: EventService, repository: MagicMock, fake_cache: InMemoryCache
) -> None:
    fake_cache.fail_get = True

    result = service.get_stats(EventFilters())

    assert result.cache_status is CacheStatus.ERROR
    assert result.stats.total == 2
    repository.get_stats.assert_called_once()
    # an unreachable cache is not written to either
    assert fake_cache.set_calls == []


def test_cache_write_failure_still_returns_stats(
    service: EventService, repository: MagicMock, fake_cache: InMemoryCache
) -> None:
    fake_cache.fail_set = True

    result = service.get_stats(EventFilters())

    # the lookup itself was a plain miss; only the write failed
    assert result.cache_status is CacheStatus.MISS
    assert result.stats.total == 2
    repository.get_stats.assert_called_once()


def test_corrupt_cache_entry_falls_back_to_database(
    service: EventService, repository: MagicMock, fake_cache: InMemoryCache
) -> None:
    fake_cache.put_raw(stats_cache_key(EventFilters()), "not stats json")

    result = service.get_stats(EventFilters())

    assert result.cache_status is CacheStatus.ERROR
    # Redis itself works, so the unreadable entry is overwritten
    assert [key for key, _, _ in fake_cache.set_calls] == [stats_cache_key(EventFilters())]
    assert result.stats.total == 2
    repository.get_stats.assert_called_once()


def test_without_cache_always_uses_database(db_session: Session) -> None:
    _seed(db_session)
    service = EventService(EventRepository(db_session))

    assert service.get_stats(EventFilters()).cache_hit is False
    assert service.get_stats(EventFilters()).stats.total == 2
