import threading
import time
from typing import Protocol

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from app.core.exceptions import PublishError

DEFAULT_PUBLISH_TIMEOUT_SECONDS = 5.0
DEFAULT_FLUSH_TIMEOUT_SECONDS = 10.0
DEFAULT_CHECK_TIMEOUT_SECONDS = 2.0


class EventPublisher(Protocol):
    def publish(self, key: str, value: bytes) -> None:
        """Publish one message and block until the broker acknowledges it.

        Raises PublishError if the message is not acknowledged.
        """
        ...

    def close(self) -> None: ...


class KafkaProducerClient:
    """A Producer plus a blocking send() that waits for the delivery report.

    Shared by the event and dead-letter publishers. confluent-kafka's
    Producer is thread-safe, so one instance can serve many threads.
    """

    def __init__(self, bootstrap_servers: str, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                # librdkafka gives up on the message at the same time send()
                # stops waiting, so a message we reported as failed is unlikely
                # to be delivered later.
                "message.timeout.ms": int(timeout_seconds * 1000),
            }
        )

    def send(
        self,
        topic: str,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        delivered = threading.Event()
        errors: list[KafkaError] = []

        def on_delivery(err: KafkaError | None, _msg: Message) -> None:
            if err is not None:
                errors.append(err)
            delivered.set()

        try:
            self._producer.produce(
                topic, key=key, value=value, headers=headers, on_delivery=on_delivery
            )
        except (BufferError, KafkaException) as exc:
            raise PublishError(f"Failed to enqueue message: {exc}") from exc

        deadline = time.monotonic() + self._timeout_seconds
        while not delivered.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PublishError(
                    f"Timed out after {self._timeout_seconds}s waiting for delivery"
                )
            # poll() serves delivery callbacks; another thread's poll() may
            # serve ours, which is why we also wait on the event.
            self._producer.poll(min(remaining, 0.1))

        if errors:
            raise PublishError(f"Delivery failed: {errors[0]}")

    def flush(self) -> None:
        self._producer.flush(DEFAULT_FLUSH_TIMEOUT_SECONDS)

    def check_topic(self, topic: str, timeout_seconds: float) -> None:
        """Raise if the broker is unreachable or the topic does not exist."""
        metadata = self._producer.list_topics(topic, timeout=timeout_seconds)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None:
            raise KafkaException(f"Topic {topic!r} not found")
        if topic_metadata.error is not None:
            raise KafkaException(topic_metadata.error)


class KafkaEventPublisher:
    """Publishes events to a Kafka topic with one long-lived Producer."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        timeout_seconds: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS,
    ) -> None:
        self._topic = topic
        self._producer = KafkaProducerClient(bootstrap_servers, timeout_seconds)

    def publish(self, key: str, value: bytes) -> None:
        self._producer.send(self._topic, key=key.encode("utf-8"), value=value)

    def check_connection(self, timeout_seconds: float = DEFAULT_CHECK_TIMEOUT_SECONDS) -> None:
        """Readiness probe: raise if the events topic cannot be reached."""
        self._producer.check_topic(self._topic, timeout_seconds)

    def close(self) -> None:
        self._producer.flush()
