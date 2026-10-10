"""End-to-end tests against the broker from docker-compose.

Needs the events and dead-letter topics (created by kafka-init).
Excluded from the default run; use: pytest -m integration
"""

import json
import time
import uuid
from collections.abc import Callable

import pytest
from confluent_kafka import Consumer, Message, Producer
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.api.events import get_event_publisher
from app.consumer.handler import MessageOutcome, handle_message
from app.consumer.runner import process_message
from app.core.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_DLQ_TOPIC, KAFKA_EVENTS_TOPIC
from app.main import app
from app.messaging.dead_letter import KafkaDeadLetterPublisher
from app.messaging.publisher import KafkaEventPublisher
from app.models.event import EventORM
from app.repositories.event_repository import EventRepository
from app.services.event_service import EventService
from tests.conftest import borrowed_session_factory

pytestmark = pytest.mark.integration

CONSUME_DEADLINE_SECONDS = 30.0


def _new_consumer(topic: str) -> Consumer:
    # A throwaway group reading from the beginning; messages left over from
    # earlier runs are skipped by the caller's predicate.
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"integration-test-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    return consumer


def _consume_until(consumer: Consumer, matches: Callable[[Message], bool]) -> Message:
    deadline = time.monotonic() + CONSUME_DEADLINE_SECONDS
    while time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is not None and message.error() is None and matches(message):
            return message
    raise AssertionError(f"No matching message within {CONSUME_DEADLINE_SECONDS}s")


def _has_event_id(event_id: str) -> Callable[[Message], bool]:
    def matches(message: Message) -> bool:
        try:
            return json.loads(message.value())["event_id"] == event_id
        except (TypeError, ValueError, KeyError):
            return False

    return matches


def test_posted_event_is_persisted_by_consumer(
    client: TestClient, db_session: Session
) -> None:
    publisher = KafkaEventPublisher(KAFKA_BOOTSTRAP_SERVERS, KAFKA_EVENTS_TOPIC)
    app.dependency_overrides[get_event_publisher] = lambda: publisher
    event_id = f"it-{uuid.uuid4()}"
    payload = {
        "event_id": event_id,
        "source": "integration-test",
        "event_type": "CPU_SPIKE",
        "severity": "HIGH",
        "timestamp": "2026-09-09T12:30:00Z",
        "value": 92.5,
    }
    try:
        response = client.post("/v1/events", json=payload)
    finally:
        publisher.close()
    assert response.status_code == 202

    consumer = _new_consumer(KAFKA_EVENTS_TOPIC)
    try:
        message = _consume_until(consumer, _has_event_id(event_id))
    finally:
        consumer.close()

    assert message.key() == b"integration-test"
    result = handle_message(message.value(), EventService(EventRepository(db_session)))
    assert result.outcome is MessageOutcome.STORED

    stored = db_session.query(EventORM).filter_by(event_id=event_id).one()
    assert stored.source == "integration-test"
    assert stored.value == 92.5


def test_invalid_message_ends_up_on_dead_letter_topic(db_session: Session) -> None:
    key = f"it-dlq-{uuid.uuid4()}".encode()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(KAFKA_EVENTS_TOPIC, key=key, value=b"not json")
    assert producer.flush(10) == 0

    events_consumer = _new_consumer(KAFKA_EVENTS_TOPIC)
    dead_letter_publisher = KafkaDeadLetterPublisher(KAFKA_BOOTSTRAP_SERVERS, KAFKA_DLQ_TOPIC)
    try:
        message = _consume_until(events_consumer, lambda m: m.key() == key)
        outcome = process_message(
            events_consumer, message, borrowed_session_factory(db_session), dead_letter_publisher
        )
    finally:
        events_consumer.close()
        dead_letter_publisher.close()
    assert outcome is MessageOutcome.REJECTED

    dlq_consumer = _new_consumer(KAFKA_DLQ_TOPIC)
    try:
        dead_letter = _consume_until(dlq_consumer, lambda m: m.key() == key)
    finally:
        dlq_consumer.close()

    assert dead_letter.value() == b"not json"
    headers = dict(dead_letter.headers() or [])
    assert headers["dlq.error.category"] == b"invalid_payload"
    assert headers["dlq.error.detail"].startswith(b"<root>: Invalid JSON")
    assert headers["dlq.original.topic"] == KAFKA_EVENTS_TOPIC.encode()
    assert headers["dlq.original.partition"] == str(message.partition()).encode()
    assert headers["dlq.original.offset"] == str(message.offset()).encode()
