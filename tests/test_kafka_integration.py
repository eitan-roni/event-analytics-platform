"""End-to-end: POST -> Kafka -> consumer logic -> row in Postgres.

Needs the broker from docker-compose (with the events topic created).
Excluded from the default run; use: pytest -m integration
"""

import json
import time
import uuid

import pytest
from confluent_kafka import Consumer
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.api.events import get_event_publisher
from app.consumer.handler import MessageOutcome, handle_message
from app.core.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_EVENTS_TOPIC
from app.main import app
from app.messaging.publisher import KafkaEventPublisher
from app.models.event import EventORM
from app.repositories.event_repository import EventRepository
from app.services.event_service import EventService

pytestmark = pytest.mark.integration

CONSUME_DEADLINE_SECONDS = 30.0


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

    # A throwaway group reading from the beginning; messages left over from
    # earlier runs are skipped until ours (unique event_id) shows up.
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"integration-test-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_EVENTS_TOPIC])
    try:
        deadline = time.monotonic() + CONSUME_DEADLINE_SECONDS
        message = None
        while time.monotonic() < deadline:
            candidate = consumer.poll(1.0)
            if candidate is None or candidate.error() is not None:
                continue
            try:
                candidate_id = json.loads(candidate.value())["event_id"]
            except (TypeError, ValueError, KeyError):
                continue
            if candidate_id == event_id:
                message = candidate
                break
        assert message is not None, f"{event_id} not consumed within {CONSUME_DEADLINE_SECONDS}s"
    finally:
        consumer.close()

    assert message.key() == b"integration-test"
    outcome = handle_message(message.value(), EventService(EventRepository(db_session)))
    assert outcome is MessageOutcome.STORED

    stored = db_session.query(EventORM).filter_by(event_id=event_id).one()
    assert stored.source == "integration-test"
    assert stored.value == 92.5
