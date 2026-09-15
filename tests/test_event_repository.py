from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models.event import EventORM
from app.repositories.event_repository import EventRepository


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
