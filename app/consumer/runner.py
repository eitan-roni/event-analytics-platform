import logging
import signal
from collections.abc import Callable
from contextlib import AbstractContextManager
from types import FrameType
from typing import Protocol

from confluent_kafka import Consumer, KafkaException, Message
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.consumer.handler import (
    MessageOutcome,
    Rejection,
    RejectionCategory,
    handle_message,
)
from app.core.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_DLQ_TOPIC,
    KAFKA_EVENTS_TOPIC,
)
from app.core.exceptions import PublishError
from app.db.session import SessionLocal
from app.messaging.dead_letter import DeadLetterPublisher, KafkaDeadLetterPublisher
from app.repositories.event_repository import EventRepository
from app.services.event_service import EventService

logger = logging.getLogger(__name__)

POLL_TIMEOUT_SECONDS = 1.0
MAX_ERROR_DETAIL_LENGTH = 1000


class ConsumedMessage(Protocol):
    """The parts of a confluent_kafka Message that process_message uses."""

    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...


class OffsetCommitter(Protocol):
    def commit(self, message: Message, asynchronous: bool) -> object: ...


def dead_letter_headers(
    rejection: Rejection, topic: str, partition: int, offset: int
) -> list[tuple[str, bytes]]:
    return [
        ("dlq.error.category", rejection.category.value.encode("utf-8")),
        ("dlq.error.detail", rejection.detail[:MAX_ERROR_DETAIL_LENGTH].encode("utf-8")),
        ("dlq.original.topic", topic.encode("utf-8")),
        ("dlq.original.partition", str(partition).encode("utf-8")),
        ("dlq.original.offset", str(offset).encode("utf-8")),
    ]


def process_message(
    consumer: OffsetCommitter,
    message: ConsumedMessage,
    session_factory: Callable[[], AbstractContextManager[Session]],
    dead_letter_publisher: DeadLetterPublisher,
) -> MessageOutcome:
    """Handle one message, then commit its offset.

    The DB transaction is committed inside handle_message, so the offset is
    only committed once the event is durably stored. A rejected message is
    first forwarded to the dead-letter topic and acknowledged by the broker.

    Errors that propagate (PublishError from the dead-letter publish, or a
    database error such as OperationalError) leave the offset uncommitted, so
    the message will be redelivered.
    """
    location = f"{message.topic()}[{message.partition()}]@{message.offset()}"
    with session_factory() as session:
        result = handle_message(message.value(), EventService(EventRepository(session)))
        if result.rejection is not None:
            session.rollback()

    if result.rejection is not None:
        level = (
            logging.WARNING
            if result.rejection.category is RejectionCategory.INVALID_PAYLOAD
            else logging.ERROR
        )
        logger.log(
            level,
            "Dead-lettering message %s (%s): %s",
            location,
            result.rejection.category.value,
            result.rejection.detail,
        )
        dead_letter_publisher.publish(
            key=message.key(),
            value=message.value(),
            headers=dead_letter_headers(
                result.rejection, message.topic(), message.partition(), message.offset()
            ),
        )
    elif result.outcome is MessageOutcome.DUPLICATE:
        logger.info("Message %s is a duplicate of a stored event; ignoring", location)

    try:
        consumer.commit(message=message, asynchronous=False)
    except KafkaException as exc:
        if exc.args[0].fatal():
            raise
        # The message will be redelivered; the insert is idempotent.
        logger.warning("Failed to commit offset for %s: %s", location, exc)
    return result.outcome


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_CONSUMER_GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_EVENTS_TOPIC])
    dead_letter_publisher = KafkaDeadLetterPublisher(KAFKA_BOOTSTRAP_SERVERS, KAFKA_DLQ_TOPIC)

    running = True

    def stop(signum: int, _frame: FrameType | None) -> None:
        nonlocal running
        logger.info("Received signal %s; shutting down", signum)
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    logger.info(
        "Consuming topic %r as group %r from %s; dead-letter topic %r",
        KAFKA_EVENTS_TOPIC,
        KAFKA_CONSUMER_GROUP,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_DLQ_TOPIC,
    )
    try:
        while running:
            message = consumer.poll(POLL_TIMEOUT_SECONDS)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.fatal():
                    logger.error("Fatal Kafka error: %s", error)
                    return 1
                logger.warning("Kafka error: %s", error)
                continue
            try:
                outcome = process_message(
                    consumer, message, SessionLocal, dead_letter_publisher
                )
            except PublishError:
                logger.exception(
                    "Dead-letter publish failed for %s[%s]@%s; exiting without "
                    "committing the offset",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                return 1
            except SQLAlchemyError:
                logger.exception(
                    "Database error at %s[%s]@%s; exiting without committing the offset",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                return 1
            except KafkaException as exc:
                # process_message only re-raises fatal commit errors
                logger.error("Fatal Kafka error while committing offset: %s", exc)
                return 1
            logger.debug(
                "%s[%s]@%s: %s",
                message.topic(),
                message.partition(),
                message.offset(),
                outcome.value,
            )
    finally:
        # Auto-commit is disabled, so close() does not commit anything.
        consumer.close()
        dead_letter_publisher.close()
    return 0
