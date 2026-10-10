import json
import logging
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
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

from app.consumer.handler import MessageOutcome
from app.consumer.runner import process_message
from app.models.event import EventORM
from tests.test_event_message_handler import VALID_MESSAGE


class _StubMessage:
    def __init__(self, value: bytes | None) -> None:
        self._value = value

    def value(self) -> bytes | None:
        return self._value

    def topic(self) -> str:
        return "events"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 42


class _RecordingConsumer:
    def __init__(self, db_session: Session) -> None:
        self._db_session = db_session
        self.committed: list[_StubMessage] = []
        self.rows_at_commit: int | None = None

    def commit(self, message: _StubMessage, asynchronous: bool) -> None:
        assert asynchronous is False
        # records whether the row was already written when the offset is committed
        self.rows_at_commit = self._db_session.query(EventORM).count()
        self.committed.append(message)


def _session_factory(session: Session) -> Callable[[], AbstractContextManager[Session]]:
    # process_message closes the session it opens; yield the given session
    # without closing it so the test fixture keeps ownership.
    @contextmanager
    def factory() -> Iterator[Session]:
        yield session

    return factory


def test_offset_committed_after_event_is_stored(db_session: Session) -> None:
    consumer = _RecordingConsumer(db_session)
    message = _StubMessage(VALID_MESSAGE)

    outcome = process_message(consumer, message, _session_factory(db_session))  # type: ignore[arg-type]

    assert outcome is MessageOutcome.STORED
    assert consumer.committed == [message]
    assert consumer.rows_at_commit == 1


def test_offset_committed_for_skipped_message(db_session: Session) -> None:
    consumer = _RecordingConsumer(db_session)
    message = _StubMessage(b"not json")

    outcome = process_message(consumer, message, _session_factory(db_session))  # type: ignore[arg-type]

    assert outcome is MessageOutcome.SKIPPED
    assert consumer.committed == [message]


def _broken_session(error_class: type[SQLAlchemyError]) -> MagicMock:
    session = MagicMock(spec=Session)
    session.execute.side_effect = error_class(  # type: ignore[call-arg]
        "INSERT ...", {}, Exception("simulated database error")
    )
    return session


@pytest.mark.parametrize("error_class", [OperationalError, InterfaceError])
def test_offset_not_committed_on_connection_error(
    db_session: Session, error_class: type[SQLAlchemyError]
) -> None:
    consumer = _RecordingConsumer(db_session)
    message = _StubMessage(VALID_MESSAGE)

    with pytest.raises(error_class):
        process_message(consumer, message, _session_factory(_broken_session(error_class)))  # type: ignore[arg-type]

    assert consumer.committed == []


@pytest.mark.parametrize("error_class", [DataError, IntegrityError])
def test_message_rejected_by_database_is_skipped_and_committed(
    db_session: Session, error_class: type[SQLAlchemyError], caplog: pytest.LogCaptureFixture
) -> None:
    consumer = _RecordingConsumer(db_session)
    message = _StubMessage(VALID_MESSAGE)
    session = _broken_session(error_class)

    with caplog.at_level(logging.ERROR, logger="app.consumer.runner"):
        outcome = process_message(consumer, message, _session_factory(session))  # type: ignore[arg-type]

    assert outcome is MessageOutcome.REJECTED
    assert consumer.committed == [message]
    session.rollback.assert_called_once()
    assert "events[0]@42" in caplog.text


class _FailingCommitConsumer:
    def __init__(self, fatal: bool) -> None:
        self._fatal = fatal

    def commit(self, message: _StubMessage, asynchronous: bool) -> None:
        raise KafkaException(
            KafkaError(KafkaError.REBALANCE_IN_PROGRESS, "simulated", fatal=self._fatal)
        )


def test_non_fatal_commit_failure_is_logged_and_processing_continues(
    db_session: Session,
) -> None:
    message = _StubMessage(VALID_MESSAGE)

    outcome = process_message(
        _FailingCommitConsumer(fatal=False), message, _session_factory(db_session)  # type: ignore[arg-type]
    )

    assert outcome is MessageOutcome.STORED


def test_fatal_commit_failure_is_raised(db_session: Session) -> None:
    message = _StubMessage(VALID_MESSAGE)

    with pytest.raises(KafkaException):
        process_message(
            _FailingCommitConsumer(fatal=True), message, _session_factory(db_session)  # type: ignore[arg-type]
        )


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


def test_nul_character_message_does_not_block_consumer(db_session: Session) -> None:
    # Regression: Postgres rejects NUL in text columns on every attempt, so a
    # message like this used to stop the consumer without committing the
    # offset and stall it on the same message after each restart.
    consumer = _RecordingConsumer(db_session)
    message = _StubMessage(NUL_MESSAGE)

    outcome = process_message(consumer, message, _session_factory(db_session))  # type: ignore[arg-type]

    assert outcome is not MessageOutcome.STORED
    assert consumer.committed == [message]
    db_session.rollback()
    assert db_session.query(EventORM).count() == 0
