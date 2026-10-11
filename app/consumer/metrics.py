"""Prometheus metrics of the consumer process, served on CONSUMER_METRICS_PORT.

Labels are fixed value sets plus topic/partition, never message content.
"""

from collections.abc import Sequence
from typing import Protocol

from confluent_kafka import OFFSET_INVALID, TopicPartition
from prometheus_client import Counter, Gauge, Histogram

MESSAGES_PROCESSED = Counter(
    "consumer_messages_processed_total",
    "Messages processed to completion, by outcome (stored, duplicate, rejected).",
    ["outcome"],
)
MESSAGES_DEAD_LETTERED = Counter(
    "consumer_messages_dead_lettered_total",
    "Messages forwarded to the dead-letter topic, by category.",
    ["category"],
)
MESSAGE_PROCESSING_DURATION = Histogram(
    "consumer_message_processing_seconds",
    "Time to process one message, including dead-lettering and the offset commit.",
)
CONSUMER_LAG = Gauge(
    "consumer_lag_messages",
    "Messages behind the end of the partition (high watermark minus position).",
    ["topic", "partition"],
)


class LagSource(Protocol):
    """The parts of a confluent_kafka Consumer used to compute lag."""

    def assignment(self) -> list[TopicPartition]: ...

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]: ...

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float | None = None, cached: bool = False
    ) -> tuple[int, int] | None: ...


def update_consumer_lag(consumer: LagSource) -> None:
    """Set the lag gauge for every assigned partition whose offsets are known.

    Uses the watermarks cached from fetch responses, so it never blocks on
    the broker; partitions without a known position or watermark are skipped.
    """
    assigned = consumer.assignment()
    if not assigned:
        return
    for partition in consumer.position(assigned):
        if partition.offset < 0:  # no position yet (OFFSET_INVALID and friends)
            continue
        watermarks = consumer.get_watermark_offsets(partition, cached=True)
        if watermarks is None:
            continue
        _low, high = watermarks
        if high == OFFSET_INVALID or high < 0:
            continue
        CONSUMER_LAG.labels(partition.topic, str(partition.partition)).set(
            max(high - partition.offset, 0)
        )


def forget_partitions(partitions: Sequence[TopicPartition]) -> None:
    """Drop the lag series of partitions this consumer no longer owns."""
    for partition in partitions:
        try:
            CONSUMER_LAG.remove(partition.topic, str(partition.partition))
        except KeyError:
            pass
