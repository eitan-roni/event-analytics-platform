from datetime import datetime, timezone

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.exceptions import DuplicateEventError
from app.models.event import EventORM
from app.repositories.event_repository import EventRepository
from app.schemas.event import EventFilters


def test_create_persists_event(db_session: Session) -> None:
    repository = EventRepository(db_session)
    event = EventORM(
        event_id="evt-001",
        source="server-01",
        event_type="CPU_SPIKE",
        severity="HIGH",
        timestamp=datetime(2026, 9, 9, 12, 30, tzinfo=timezone.utc),
        value=92.5,
    )

    persisted = repository.create(event)

    assert persisted.id is not None
    fetched = db_session.get(EventORM, persisted.id)
    assert fetched is not None
    assert fetched.event_id == "evt-001"
    assert fetched.source == "server-01"
    assert fetched.severity == "HIGH"
    assert fetched.value == 92.5


def test_create_duplicate_event_id_raises_and_rolls_back(db_session: Session) -> None:
    repository = EventRepository(db_session)
    first = EventORM(
        event_id="evt-dup",
        source="server-01",
        event_type="CPU_SPIKE",
        severity="HIGH",
        timestamp=datetime(2026, 9, 9, 12, 30, tzinfo=timezone.utc),
        value=92.5,
    )
    repository.create(first)

    duplicate = EventORM(
        event_id="evt-dup",
        source="server-02",
        event_type="MEM_SPIKE",
        severity="LOW",
        timestamp=datetime(2026, 9, 9, 13, 0, tzinfo=timezone.utc),
        value=1.0,
    )
    with pytest.raises(DuplicateEventError) as exc_info:
        repository.create(duplicate)
    assert exc_info.value.event_id == "evt-dup"

    # the session is still usable after the repository's own rollback
    assert db_session.query(EventORM).filter_by(event_id="evt-dup").count() == 1


def test_create_non_unique_integrity_error_is_not_swallowed(db_session: Session) -> None:
    repository = EventRepository(db_session)
    # Bypasses pydantic validation to trigger a NOT NULL violation directly
    # against the database — a different IntegrityError than the unique
    # violation on event_id, which must not be reported as a duplicate.
    event = EventORM(
        event_id="evt-not-null",
        source="server-01",
        event_type="CPU_SPIKE",
        severity=None,  # type: ignore[arg-type]
        timestamp=datetime(2026, 9, 9, 12, 30, tzinfo=timezone.utc),
        value=92.5,
    )

    with pytest.raises(IntegrityError):
        repository.create(event)

    # the session is still usable after the repository's own rollback
    assert db_session.query(EventORM).filter_by(event_id="evt-not-null").count() == 0


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


def test_list_orders_by_timestamp_descending(db_session: Session) -> None:
    _seed(db_session)
    repository = EventRepository(db_session)

    results = repository.list_events(EventFilters(), limit=50, offset=0)

    assert [event.event_id for event in results] == ["evt-2", "evt-1"]


def test_list_applies_pagination(db_session: Session) -> None:
    _seed(db_session)
    repository = EventRepository(db_session)

    results = repository.list_events(EventFilters(), limit=1, offset=1)

    assert [event.event_id for event in results] == ["evt-1"]


def test_count_matches_filters(db_session: Session) -> None:
    _seed(db_session)
    repository = EventRepository(db_session)

    assert repository.count(EventFilters()) == 2
    assert repository.count(EventFilters(source="server-02")) == 1
    assert repository.count(EventFilters(source="server-99")) == 0


def test_get_stats_on_empty_table(db_session: Session) -> None:
    repository = EventRepository(db_session)

    total, avg_value, min_value, max_value, by_severity, by_event_type = (
        repository.get_stats(EventFilters())
    )

    assert total == 0
    assert avg_value is None
    assert min_value is None
    assert max_value is None
    assert by_severity == {}
    assert by_event_type == {}


def test_get_stats_aggregates_in_sql(db_session: Session) -> None:
    _seed(db_session)
    repository = EventRepository(db_session)

    total, avg_value, min_value, max_value, by_severity, by_event_type = (
        repository.get_stats(EventFilters())
    )

    assert total == 2
    assert avg_value == 15.0
    assert min_value == 10.0
    assert max_value == 20.0
    assert by_severity == {"LOW": 1, "HIGH": 1}
    assert by_event_type == {"CPU_SPIKE": 1, "MEM_SPIKE": 1}
