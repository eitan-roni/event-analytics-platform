from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import OperationalError
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


def test_offset_not_committed_on_database_error(db_session: Session) -> None:
    consumer = _RecordingConsumer(db_session)
    message = _StubMessage(VALID_MESSAGE)

    broken_session = MagicMock(spec=Session)
    broken_session.execute.side_effect = OperationalError(
        "INSERT ...", {}, Exception("connection lost")
    )

    with pytest.raises(OperationalError):
        process_message(consumer, message, _session_factory(broken_session))  # type: ignore[arg-type]

    assert consumer.committed == []
