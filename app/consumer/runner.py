import logging
import signal
from collections.abc import Callable
from types import FrameType
from typing import Protocol

from confluent_kafka import Consumer, Message
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.consumer.handler import MessageOutcome, handle_message
from app.core.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_EVENTS_TOPIC,
)
from app.db.base import Base
from app.db.session import SessionLocal, engine
from app.repositories.event_repository import EventRepository
from app.services.event_service import EventService

logger = logging.getLogger(__name__)

POLL_TIMEOUT_SECONDS = 1.0


class OffsetCommitter(Protocol):
    def commit(self, message: Message, asynchronous: bool) -> object: ...


def process_message(
    consumer: OffsetCommitter,
    message: Message,
    session_factory: Callable[[], Session],
) -> MessageOutcome:
    """Handle one message, then commit its offset.

    The DB transaction is committed inside handle_message, so the offset is
    only committed once the event is durably stored (or deliberately skipped).
    A database error propagates before the commit, so the message will be
    redelivered.
    """
    with session_factory() as session:
        outcome = handle_message(message.value(), EventService(EventRepository(session)))
    consumer.commit(message=message, asynchronous=False)
    return outcome


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    Base.metadata.create_all(bind=engine)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_CONSUMER_GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_EVENTS_TOPIC])

    running = True

    def stop(signum: int, _frame: FrameType | None) -> None:
        nonlocal running
        logger.info("Received signal %s; shutting down", signum)
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    logger.info(
        "Consuming topic %r as group %r from %s",
        KAFKA_EVENTS_TOPIC,
        KAFKA_CONSUMER_GROUP,
        KAFKA_BOOTSTRAP_SERVERS,
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
                outcome = process_message(consumer, message, SessionLocal)
            except SQLAlchemyError:
                logger.exception(
                    "Database error at %s[%s]@%s; exiting without committing the offset",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
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
    return 0
