from typing import Protocol

from app.messaging.publisher import DEFAULT_PUBLISH_TIMEOUT_SECONDS, KafkaProducerClient


class DeadLetterPublisher(Protocol):
    def publish(
        self, key: bytes | None, value: bytes | None, headers: list[tuple[str, bytes]]
    ) -> None:
        """Forward one message and block until the broker acknowledges it.

        Key and value are the original raw bytes (either may be missing).
        Raises PublishError if the message is not acknowledged.
        """
        ...

    def close(self) -> None: ...


class KafkaDeadLetterPublisher:
    """Forwards unprocessable messages to the dead-letter topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        timeout_seconds: float = DEFAULT_PUBLISH_TIMEOUT_SECONDS,
    ) -> None:
        self._topic = topic
        self._producer = KafkaProducerClient(bootstrap_servers, timeout_seconds)

    def publish(
        self, key: bytes | None, value: bytes | None, headers: list[tuple[str, bytes]]
    ) -> None:
        self._producer.send(self._topic, key=key, value=value, headers=headers)

    def close(self) -> None:
        self._producer.flush()
