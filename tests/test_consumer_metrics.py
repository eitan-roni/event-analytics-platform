import pytest
from confluent_kafka import OFFSET_INVALID, TopicPartition
from prometheus_client import REGISTRY
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.consumer.metrics import forget_partitions, update_consumer_lag
from app.consumer.runner import process_message
from app.core.exceptions import PublishError
from tests.conftest import InMemoryDeadLetterPublisher, borrowed_session_factory
from tests.test_consumer_runner import _broken_session, _StubMessage
from tests.test_event_message_handler import VALID_MESSAGE

OUTCOMES = ("stored", "duplicate", "rejected")
CATEGORIES = ("invalid_payload", "database_rejected")


def _value(name: str, **labels: str) -> float:
    # Counters are process-global, so tests compare before/after deltas.
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _snapshot() -> dict[str, float]:
    values = {
        f"outcome:{o}": _value("consumer_messages_processed_total", outcome=o) for o in OUTCOMES
    }
    values |= {
        f"category:{c}": _value("consumer_messages_dead_lettered_total", category=c)
        for c in CATEGORIES
    }
    values["duration_count"] = _value("consumer_message_processing_seconds_count")
    return values


def _deltas(before: dict[str, float]) -> dict[str, float]:
    return {key: value - before[key] for key, value in _snapshot().items() if value != before[key]}


class _Consumer:
    def commit(self, message: object, asynchronous: bool) -> None:
        pass


def test_processed_messages_are_counted_by_outcome(db_session: Session) -> None:
    dead_letters = InMemoryDeadLetterPublisher()
    factory = borrowed_session_factory(db_session)
    before = _snapshot()

    process_message(_Consumer(), _StubMessage(VALID_MESSAGE), factory, dead_letters)  # stored
    process_message(_Consumer(), _StubMessage(VALID_MESSAGE), factory, dead_letters)  # duplicate
    process_message(_Consumer(), _StubMessage(b"not json"), factory, dead_letters)  # rejected

    assert _deltas(before) == {
        "outcome:stored": 1,
        "outcome:duplicate": 1,
        "outcome:rejected": 1,
        "category:invalid_payload": 1,
        "duration_count": 3,
    }


@pytest.mark.parametrize("error_class", [DataError, IntegrityError])
def test_database_rejections_are_counted_by_category(
    error_class: type[SQLAlchemyError],
) -> None:
    session = _broken_session(error_class)
    before = _snapshot()

    process_message(
        _Consumer(),
        _StubMessage(VALID_MESSAGE),
        borrowed_session_factory(session),
        InMemoryDeadLetterPublisher(),
    )

    assert _deltas(before) == {
        "outcome:rejected": 1,
        "category:database_rejected": 1,
        "duration_count": 1,
    }


def test_failed_dead_letter_publish_counts_nothing(db_session: Session) -> None:
    dead_letters = InMemoryDeadLetterPublisher()
    dead_letters.fail = True
    before = _snapshot()

    with pytest.raises(PublishError):
        process_message(
            _Consumer(),
            _StubMessage(b"not json"),
            borrowed_session_factory(db_session),
            dead_letters,
        )

    assert _deltas(before) == {}


class _LagConsumer:
    """Fake consumer with fixed positions and cached watermarks."""

    def __init__(
        self, positions: dict[int, int], high_watermarks: dict[int, int | None]
    ) -> None:
        self._positions = positions
        self._high_watermarks = high_watermarks

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition("lag-test", p) for p in self._positions]

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        return [
            TopicPartition(tp.topic, tp.partition, self._positions[tp.partition])
            for tp in partitions
        ]

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float | None = None, cached: bool = False
    ) -> tuple[int, int] | None:
        assert cached is True  # must not block on the broker
        high = self._high_watermarks[partition.partition]
        return None if high is None else (0, high)


def _lag(partition: int) -> float | None:
    return REGISTRY.get_sample_value(
        "consumer_lag_messages", {"topic": "lag-test", "partition": str(partition)}
    )


def test_lag_is_set_per_partition_and_unknown_offsets_are_skipped() -> None:
    consumer = _LagConsumer(
        positions={0: 10, 1: 50, 2: OFFSET_INVALID, 3: 5},
        high_watermarks={0: 25, 1: 50, 2: 100, 3: None},
    )
    try:
        update_consumer_lag(consumer)

        assert _lag(0) == 15
        assert _lag(1) == 0
        assert _lag(2) is None  # no position yet
        assert _lag(3) is None  # watermark not known yet
    finally:
        forget_partitions(consumer.assignment())


def test_revoked_partitions_are_removed_from_the_gauge() -> None:
    consumer = _LagConsumer(positions={0: 1, 1: 1}, high_watermarks={0: 3, 1: 4})
    update_consumer_lag(consumer)

    forget_partitions([TopicPartition("lag-test", 0)])

    assert _lag(0) is None
    assert _lag(1) == 3
    forget_partitions([TopicPartition("lag-test", 1), TopicPartition("lag-test", 9)])
    assert _lag(1) is None
