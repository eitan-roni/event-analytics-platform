import json
import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException
from sqlalchemy.exc import (
    DataError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    SQLAlchemyError,
)
from sqlalchemy.orm import Session

from app.consumer.handler import MessageOutcome, Rejection, RejectionCategory
from app.consumer.runner import MAX_ERROR_DETAIL_LENGTH, dead_letter_headers, process_message
from app.core.exceptions import PublishError
from app.models.event import EventORM
from tests.conftest import InMemoryDeadLetterPublisher, borrowed_session_factory
from tests.test_event_message_handler import VALID_MESSAGE

MESSAGE_KEY = b"server-01"

SessionFactory = Callable[[], AbstractContextManager[Session]]


class _StubMessage:
    def __init__(self, value: bytes | None, key: bytes | None = MESSAGE_KEY) -> None:
        self._value = value
        self._key = key

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def topic(self) -> str:
        return "events"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 42


class _RecordingConsumer:
    def __init__(self, db_session: Session, call_log: list[str]) -> None:
        self._db_session = db_session
        self._call_log = call_log
        self.committed: list[_StubMessage] = []
        self.rows_at_commit: int | None = None

    def commit(self, message: _StubMessage, asynchronous: bool) -> None:
        assert asynchronous is False
        # records whether the row was already written when the offset is committed
        self.rows_at_commit = self._db_session.query(EventORM).count()
        self._call_log.append("commit")
        self.committed.append(message)


def _broken_session(error_class: type[SQLAlchemyError]) -> MagicMock:
    session = MagicMock(spec=Session)
    session.execute.side_effect = error_class(  # type: ignore[call-arg]
        "INSERT ...", {}, Exception("simulated database error")
    )
    return session


def _expected_headers(category: str, detail: str) -> list[tuple[str, bytes]]:
    return [
        ("dlq.error.category", category.encode()),
        ("dlq.error.detail", detail.encode()),
        ("dlq.original.topic", b"events"),
        ("dlq.original.partition", b"0"),
        ("dlq.original.offset", b"42"),
    ]


@pytest.fixture
def session_factory(db_session: Session) -> SessionFactory:
    return borrowed_session_factory(db_session)


@pytest.fixture
def call_log() -> list[str]:
    return []


@pytest.fixture
def consumer(db_session: Session, call_log: list[str]) -> _RecordingConsumer:
    return _RecordingConsumer(db_session, call_log)


@pytest.fixture
def dead_letters(call_log: list[str]) -> InMemoryDeadLetterPublisher:
    return InMemoryDeadLetterPublisher(call_log)


def test_stored_message_is_committed_and_not_dead_lettered(
    session_factory: SessionFactory,
    consumer: _RecordingConsumer,
    dead_letters: InMemoryDeadLetterPublisher,
) -> None:
    message = _StubMessage(VALID_MESSAGE)

    outcome = process_message(consumer, message, session_factory, dead_letters)

    assert outcome is MessageOutcome.STORED
    assert consumer.committed == [message]
    assert consumer.rows_at_commit == 1
    assert dead_letters.messages == []


def test_duplicate_message_is_committed_and_not_dead_lettered(
    session_factory: SessionFactory,
    consumer: _RecordingConsumer,
    dead_letters: InMemoryDeadLetterPublisher,
) -> None:
    process_message(consumer, _StubMessage(VALID_MESSAGE), session_factory, dead_letters)

    outcome = process_message(
        consumer, _StubMessage(VALID_MESSAGE), session_factory, dead_letters
    )

    assert outcome is MessageOutcome.DUPLICATE
    assert len(consumer.committed) == 2
    assert dead_letters.messages == []


def test_invalid_payload_is_dead_lettered_then_committed(
    session_factory: SessionFactory,
    consumer: _RecordingConsumer,
    dead_letters: InMemoryDeadLetterPublisher,
    call_log: list[str],
) -> None:
    message = _StubMessage(b"not json")

    outcome = process_message(consumer, message, session_factory, dead_letters)

    assert outcome is MessageOutcome.REJECTED
    assert len(dead_letters.messages) == 1
    key, value, headers = dead_letters.messages[0]
    assert key == MESSAGE_KEY
    assert value == b"not json"
    # the detail is pydantic's message; check its prefix, everything else exactly
    detail = dict(headers)["dlq.error.detail"].decode()
    assert detail.startswith("<root>: Invalid JSON")
    assert headers == _expected_headers("invalid_payload", detail)
    assert call_log == ["dead_letter", "commit"]


@pytest.mark.parametrize("error_class", [DataError, IntegrityError])
def test_database_rejected_message_is_dead_lettered_then_committed(
    consumer: _RecordingConsumer,
    dead_letters: InMemoryDeadLetterPublisher,
    call_log: list[str],
    error_class: type[SQLAlchemyError],
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _StubMessage(VALID_MESSAGE)
    session = _broken_session(error_class)

    with caplog.at_level(logging.ERROR, logger="app.consumer.runner"):
        outcome = process_message(
            consumer, message, borrowed_session_factory(session), dead_letters
        )

    assert outcome is MessageOutcome.REJECTED
    session.rollback.assert_called_once()
    assert dead_letters.messages == [
        (
            MESSAGE_KEY,
            VALID_MESSAGE,
            _expected_headers("database_rejected", "Exception: simulated database error"),
        )
    ]
    assert call_log == ["dead_letter", "commit"]
    assert "events[0]@42" in caplog.text


def test_message_without_value_is_dead_lettered_without_raising(
    session_factory: SessionFactory,
    consumer: _RecordingConsumer,
    dead_letters: InMemoryDeadLetterPublisher,
) -> None:
    message = _StubMessage(None, key=None)

    outcome = process_message(consumer, message, session_factory, dead_letters)

    assert outcome is MessageOutcome.REJECTED
    assert dead_letters.messages == [
        (None, None, _expected_headers("invalid_payload", "message has no value"))
    ]
    assert consumer.committed == [message]


def test_offset_not_committed_when_dead_letter_publish_fails(
    session_factory: SessionFactory,
    consumer: _RecordingConsumer,
    dead_letters: InMemoryDeadLetterPublisher,
) -> None:
    dead_letters.fail = True
    message = _StubMessage(b"not json")

    with pytest.raises(PublishError):
        process_message(consumer, message, session_factory, dead_letters)

    assert consumer.committed == []


@pytest.mark.parametrize("error_class", [OperationalError, InterfaceError])
def test_offset_not_committed_on_connection_error(
    consumer: _RecordingConsumer,
    dead_letters: InMemoryDeadLetterPublisher,
    error_class: type[SQLAlchemyError],
) -> None:
    message = _StubMessage(VALID_MESSAGE)
    factory = borrowed_session_factory(_broken_session(error_class))

    with pytest.raises(error_class):
        process_message(consumer, message, factory, dead_letters)

    assert consumer.committed == []
    assert dead_letters.messages == []


NUL_MESSAGE = json.dumps(
    {
        "event_id": "evt-nul",
        "source": "server\u000001",
        "event_type": "CPU_SPIKE",
        "severity": "HIGH",
        "timestamp": "2026-09-09T12:30:00Z",
        "value": 92.5,
    }
).encode("utf-8")


def test_nul_character_message_does_not_block_consumer(
    db_session: Session,
    session_factory: SessionFactory,
    consumer: _RecordingConsumer,
    dead_letters: InMemoryDeadLetterPublisher,
) -> None:
    # Regression: Postgres rejects NUL in text columns on every attempt, so a
    # message like this used to stop the consumer without committing the
    # offset and stall it on the same message after each restart.
    message = _StubMessage(NUL_MESSAGE)

    outcome = process_message(consumer, message, session_factory, dead_letters)

    assert outcome is MessageOutcome.REJECTED
    assert consumer.committed == [message]
    assert [value for _, value, _ in dead_letters.messages] == [NUL_MESSAGE]
    db_session.rollback()
    assert db_session.query(EventORM).count() == 0


def test_error_detail_header_is_truncated() -> None:
    rejection = Rejection(
        RejectionCategory.INVALID_PAYLOAD, "x" * (MAX_ERROR_DETAIL_LENGTH + 500)
    )

    headers = dict(dead_letter_headers(rejection, "events", 1, 7))

    assert headers["dlq.error.detail"] == b"x" * MAX_ERROR_DETAIL_LENGTH


class _FailingCommitConsumer:
    def __init__(self, fatal: bool) -> None:
        self._fatal = fatal

    def commit(self, message: _StubMessage, asynchronous: bool) -> None:
        raise KafkaException(
            KafkaError(KafkaError.REBALANCE_IN_PROGRESS, "simulated", fatal=self._fatal)
        )


def test_non_fatal_commit_failure_is_logged_and_processing_continues(
    session_factory: SessionFactory, dead_letters: InMemoryDeadLetterPublisher
) -> None:
    consumer = _FailingCommitConsumer(fatal=False)
    message = _StubMessage(VALID_MESSAGE)

    outcome = process_message(consumer, message, session_factory, dead_letters)

    assert outcome is MessageOutcome.STORED


def test_fatal_commit_failure_is_raised(
    session_factory: SessionFactory, dead_letters: InMemoryDeadLetterPublisher
) -> None:
    consumer = _FailingCommitConsumer(fatal=True)
    message = _StubMessage(VALID_MESSAGE)

    with pytest.raises(KafkaException):
        process_message(consumer, message, session_factory, dead_letters)
