import json

import pytest
from sqlalchemy.exc import DataError, IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.consumer.handler import (
    HandlingResult,
    MessageOutcome,
    RejectionCategory,
    handle_message,
)
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
    result = handle_message(VALID_MESSAGE, _service(db_session))

    assert result == HandlingResult(MessageOutcome.STORED)
    stored = db_session.query(EventORM).filter_by(event_id="evt-msg-1").one()
    assert stored.source == "server-01"
    assert stored.severity == "HIGH"
    assert stored.value == 92.5


def test_duplicate_message_leaves_exactly_one_row(db_session: Session) -> None:
    service = _service(db_session)

    assert handle_message(VALID_MESSAGE, service).outcome is MessageOutcome.STORED
    assert handle_message(VALID_MESSAGE, service) == HandlingResult(MessageOutcome.DUPLICATE)

    assert db_session.query(EventORM).filter_by(event_id="evt-msg-1").count() == 1


@pytest.mark.parametrize(
    ("raw", "expected_detail"),
    [
        pytest.param(b"not json", "<root>: Invalid JSON", id="invalid-json"),
        pytest.param(b"\xff\xfe\x00", "<root>: Invalid JSON", id="invalid-utf8"),
        pytest.param(
            b'{"event_id": "evt-x", "severity": "URGENT"}',
            "source: Field required",
            id="invalid-schema",
        ),
        pytest.param(b"[]", "<root>: ", id="wrong-json-type"),
        pytest.param(
            VALID_MESSAGE.replace(b"server-01", b"server\\u000001"),
            "source: Value error, must not contain NUL characters",
            id="nul-in-source",
        ),
        pytest.param(
            VALID_MESSAGE.replace(b'"evt-msg-1"', b'""'), "event_id: ", id="empty-event-id"
        ),
        pytest.param(
            VALID_MESSAGE.replace(b"92.5", b"1e400"), "value: ", id="infinite-value"
        ),
        pytest.param(None, "message has no value", id="no-value"),
    ],
)
def test_malformed_message_is_rejected_as_invalid_payload(
    db_session: Session, raw: bytes | None, expected_detail: str
) -> None:
    result = handle_message(raw, _service(db_session))

    assert result.outcome is MessageOutcome.REJECTED
    assert result.rejection is not None
    assert result.rejection.category is RejectionCategory.INVALID_PAYLOAD
    assert result.rejection.detail.startswith(expected_detail)
    assert db_session.query(EventORM).count() == 0


class _FailingService:
    def __init__(self, error_class: type[SQLAlchemyError]) -> None:
        self._error_class = error_class

    def store(self, event: EventCreate) -> bool:
        raise self._error_class(  # type: ignore[call-arg]
            "INSERT ...", {}, Exception("simulated database error")
        )


@pytest.mark.parametrize("error_class", [DataError, IntegrityError])
def test_message_specific_database_error_is_rejected(
    error_class: type[SQLAlchemyError],
) -> None:
    result = handle_message(VALID_MESSAGE, _FailingService(error_class))  # type: ignore[arg-type]

    assert result.outcome is MessageOutcome.REJECTED
    assert result.rejection is not None
    assert result.rejection.category is RejectionCategory.DATABASE_REJECTED
    assert result.rejection.detail == "Exception: simulated database error"


def test_connection_level_database_error_propagates() -> None:
    with pytest.raises(OperationalError):
        handle_message(VALID_MESSAGE, _FailingService(OperationalError))  # type: ignore[arg-type]
