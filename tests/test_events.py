from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.event import EventORM

VALID_PAYLOAD = {
    "event_id": "evt-123",
    "source": "server-01",
    "event_type": "CPU_SPIKE",
    "severity": "HIGH",
    "timestamp": "2026-09-09T12:30:00Z",
    "value": 92.5,
}


def test_create_event_accepted(client: TestClient, db_session: Session) -> None:
    response = client.post("/v1/events", json=VALID_PAYLOAD)
    assert response.status_code == 201
    assert response.json() == {"event_id": "evt-123", "status": "accepted"}

    stored = db_session.query(EventORM).filter_by(event_id="evt-123").one_or_none()
    assert stored is not None
    assert stored.source == "server-01"
    assert stored.event_type == "CPU_SPIKE"
    assert stored.value == 92.5


def test_create_event_missing_field(client: TestClient) -> None:
    payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "source"}
    response = client.post("/v1/events", json=payload)
    assert response.status_code == 422


def test_create_event_invalid_severity(client: TestClient) -> None:
    payload = {**VALID_PAYLOAD, "severity": "URGENT"}
    response = client.post("/v1/events", json=payload)
    assert response.status_code == 422


def test_create_event_invalid_timestamp(client: TestClient) -> None:
    payload = {**VALID_PAYLOAD, "timestamp": "not-a-timestamp"}
    response = client.post("/v1/events", json=payload)
    assert response.status_code == 422
