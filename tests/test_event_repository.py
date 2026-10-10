from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models.event import EventORM
from app.repositories.event_repository import EventRepository
from app.schemas.event import EventFilters


def _event(event_id: str = "evt-001", **overrides: object) -> EventORM:
    fields: dict[str, object] = {
        "event_id": event_id,
        "source": "server-01",
        "event_type": "CPU_SPIKE",
        "severity": "HIGH",
        "timestamp": datetime(2026, 9, 9, 12, 30, tzinfo=timezone.utc),
        "value": 92.5,
    }
    fields.update(overrides)
    return EventORM(**fields)


def test_insert_if_absent_persists_event(db_session: Session) -> None:
    repository = EventRepository(db_session)

    assert repository.insert_if_absent(_event()) is True

    fetched = db_session.query(EventORM).filter_by(event_id="evt-001").one()
    assert fetched.id is not None
    assert fetched.source == "server-01"
    assert fetched.severity == "HIGH"
    assert fetched.value == 92.5


def test_insert_if_absent_is_idempotent(db_session: Session) -> None:
    repository = EventRepository(db_session)
    assert repository.insert_if_absent(_event("evt-dup")) is True

    duplicate = _event("evt-dup", source="server-02", severity="LOW", value=1.0)
    assert repository.insert_if_absent(duplicate) is False

    rows = db_session.query(EventORM).filter_by(event_id="evt-dup").all()
    assert len(rows) == 1
    # the first write wins; the duplicate does not overwrite it
    assert rows[0].source == "server-01"
    assert rows[0].value == 92.5


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
