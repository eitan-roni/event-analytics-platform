from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

VALID_PAYLOAD = {
    "event_id": "evt-123",
    "source": "server-01",
    "event_type": "CPU_SPIKE",
    "severity": "HIGH",
    "timestamp": "2026-09-09T12:30:00Z",
    "value": 92.5,
}


def test_create_event_accepted() -> None:
    response = client.post("/v1/events", json=VALID_PAYLOAD)
    assert response.status_code == 201
    assert response.json() == {"event_id": "evt-123", "status": "accepted"}


def test_create_event_missing_field() -> None:
    payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "source"}
    response = client.post("/v1/events", json=payload)
    assert response.status_code == 422


def test_create_event_invalid_severity() -> None:
    payload = {**VALID_PAYLOAD, "severity": "URGENT"}
    response = client.post("/v1/events", json=payload)
    assert response.status_code == 422


def test_create_event_invalid_timestamp() -> None:
    payload = {**VALID_PAYLOAD, "timestamp": "not-a-timestamp"}
    response = client.post("/v1/events", json=payload)
    assert response.status_code == 422
