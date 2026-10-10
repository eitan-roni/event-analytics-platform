import json

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.consumer.handler import MessageOutcome, handle_message
from app.models.event import EventORM
from app.repositories.event_repository import EventRepository
from app.schemas.event import EventCreate
from app.services.event_service import EventService

VALID_MESSAGE = json.dumps(
    {
        "event_id": "evt-msg-1",
        "source": "server-01",
        "event_type": "CPU_SPIKE",
        "severity": "HIGH",
        "timestamp": "2026-09-09T12:30:00Z",
        "value": 92.5,
    }
).encode("utf-8")


def _service(db_session: Session) -> EventService:
    return EventService(EventRepository(db_session))


def test_valid_message_is_persisted(db_session: Session) -> None:
    outcome = handle_message(VALID_MESSAGE, _service(db_session))

    assert outcome is MessageOutcome.STORED
    stored = db_session.query(EventORM).filter_by(event_id="evt-msg-1").one()
    assert stored.source == "server-01"
    assert stored.severity == "HIGH"
    assert stored.value == 92.5


def test_duplicate_message_leaves_exactly_one_row(db_session: Session) -> None:
    service = _service(db_session)

    assert handle_message(VALID_MESSAGE, service) is MessageOutcome.STORED
    assert handle_message(VALID_MESSAGE, service) is MessageOutcome.DUPLICATE

    assert db_session.query(EventORM).filter_by(event_id="evt-msg-1").count() == 1


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(b"not json", id="invalid-json"),
        pytest.param(b"\xff\xfe\x00", id="invalid-utf8"),
        pytest.param(b'{"event_id": "evt-x", "severity": "URGENT"}', id="invalid-schema"),
        pytest.param(b"[]", id="wrong-json-type"),
        pytest.param(VALID_MESSAGE.replace(b"server-01", b"server\u000001"), id="nul-in-source"),
        pytest.param(VALID_MESSAGE.replace(b'"evt-msg-1"', b'""'), id="empty-event-id"),
        pytest.param(VALID_MESSAGE.replace(b"92.5", b"1e400"), id="infinite-value"),
        pytest.param(None, id="no-value"),
    ],
)
def test_malformed_message_is_skipped(db_session: Session, raw: bytes | None) -> None:
    outcome = handle_message(raw, _service(db_session))

    assert outcome is MessageOutcome.SKIPPED
    assert db_session.query(EventORM).count() == 0


class _FailingService:
    def store(self, event: EventCreate) -> bool:
        raise OperationalError("INSERT ...", {}, Exception("connection lost"))


def test_database_error_propagates() -> None:
    with pytest.raises(OperationalError):
        handle_message(VALID_MESSAGE, _FailingService())  # type: ignore[arg-type]
