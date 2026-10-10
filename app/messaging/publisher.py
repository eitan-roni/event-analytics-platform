import threading
import time
from typing import Protocol

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from app.core.exceptions import PublishError

DEFAULT_PUBLISH_TIMEOUT_SECONDS = 5.0
DEFAULT_FLUSH_TIMEOUT_SECONDS = 10.0


class EventPublisher(Protocol):
    def publish(self, key: str, value: bytes) -> None:
        """Publish one message and block until the broker acknowledges it.

        Raises PublishError if the message is not acknowledged.
        """
        ...

    def close(self) -> None: ...


class KafkaEventPublisher:
    """Publishes events to a Kafka topic with one long-lived Producer.

    confluent-kafka's Producer is thread-safe, so a single instance is shared
    by all request-handling threads.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        timeout_seconds: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS,
    ) -> None:
        self._topic = topic
        self._timeout_seconds = timeout_seconds
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                # librdkafka gives up on the message at the same time publish()
                # stops waiting, so a message we reported as failed is unlikely
                # to be delivered later.
                "message.timeout.ms": int(timeout_seconds * 1000),
            }
        )

    def publish(self, key: str, value: bytes) -> None:
        delivered = threading.Event()
        errors: list[KafkaError] = []

        def on_delivery(err: KafkaError | None, _msg: Message) -> None:
            if err is not None:
                errors.append(err)
            delivered.set()

        try:
            self._producer.produce(
                self._topic, key=key.encode("utf-8"), value=value, on_delivery=on_delivery
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

    def close(self) -> None:
        self._producer.flush(DEFAULT_FLUSH_TIMEOUT_SECONDS)
