from datetime import datetime, timezone

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


def test_create_event_duplicate_event_id_returns_409(
    client: TestClient, db_session: Session
) -> None:
    first = client.post("/v1/events", json=VALID_PAYLOAD)
    assert first.status_code == 201

    duplicate = client.post("/v1/events", json=VALID_PAYLOAD)
    assert duplicate.status_code == 409
    assert "evt-123" in duplicate.json()["detail"]

    # exactly one row persisted, and the shared session is still usable
    # afterwards (the repository rolled back its own failed commit).
    assert db_session.query(EventORM).filter_by(event_id="evt-123").count() == 1


def _seed_events(db_session: Session) -> list[EventORM]:
    events = [
        EventORM(
            event_id="evt-1",
            source="server-01",
            event_type="CPU_SPIKE",
            severity="LOW",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            value=10.0,
        ),
        EventORM(
            event_id="evt-2",
            source="server-01",
            event_type="CPU_SPIKE",
            severity="HIGH",
            timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
            value=20.0,
        ),
        EventORM(
            event_id="evt-3",
            source="server-02",
            event_type="MEM_SPIKE",
            severity="HIGH",
            timestamp=datetime(2026, 1, 3, tzinfo=timezone.utc),
            value=30.0,
        ),
        EventORM(
            event_id="evt-4",
            source="server-02",
            event_type="MEM_SPIKE",
            severity="CRITICAL",
            timestamp=datetime(2026, 1, 4, tzinfo=timezone.utc),
            value=40.0,
        ),
    ]
    db_session.add_all(events)
    db_session.commit()
    return events


class TestListEvents:
    def test_default_pagination_sorted_descending(
        self, client: TestClient, db_session: Session
    ) -> None:
        _seed_events(db_session)
        response = client.get("/v1/events")
        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 4
        assert [item["event_id"] for item in body["items"]] == [
            "evt-4",
            "evt-3",
            "evt-2",
            "evt-1",
        ]

    def test_filter_by_source(self, client: TestClient, db_session: Session) -> None:
        _seed_events(db_session)
        response = client.get("/v1/events", params={"source": "server-02"})
        body = response.json()
        assert body["total"] == 2
        assert {item["event_id"] for item in body["items"]} == {"evt-3", "evt-4"}

    def test_filter_by_event_type(self, client: TestClient, db_session: Session) -> None:
        _seed_events(db_session)
        response = client.get("/v1/events", params={"event_type": "MEM_SPIKE"})
        body = response.json()
        assert body["total"] == 2
        assert {item["event_id"] for item in body["items"]} == {"evt-3", "evt-4"}

    def test_filter_by_severity(self, client: TestClient, db_session: Session) -> None:
        _seed_events(db_session)
        response = client.get("/v1/events", params={"severity": "HIGH"})
        body = response.json()
        assert body["total"] == 2
        assert {item["event_id"] for item in body["items"]} == {"evt-2", "evt-3"}

    def test_filter_by_time_range(self, client: TestClient, db_session: Session) -> None:
        _seed_events(db_session)
        response = client.get(
            "/v1/events",
            params={
                "start_time": "2026-01-02T00:00:00Z",
                "end_time": "2026-01-03T00:00:00Z",
            },
        )
        body = response.json()
        assert body["total"] == 2
        assert {item["event_id"] for item in body["items"]} == {"evt-2", "evt-3"}

    def test_combined_filters(self, client: TestClient, db_session: Session) -> None:
        _seed_events(db_session)
        response = client.get(
            "/v1/events", params={"source": "server-02", "severity": "CRITICAL"}
        )
        body = response.json()
        assert body["total"] == 1
        assert body["items"][0]["event_id"] == "evt-4"

    def test_pagination_limit_and_offset(
        self, client: TestClient, db_session: Session
    ) -> None:
        _seed_events(db_session)
        response = client.get("/v1/events", params={"limit": 2, "offset": 1})
        body = response.json()
        assert body["total"] == 4
        assert [item["event_id"] for item in body["items"]] == ["evt-3", "evt-2"]

    def test_limit_bounds_are_accepted(
        self, client: TestClient, db_session: Session
    ) -> None:
        _seed_events(db_session)
        assert client.get("/v1/events", params={"limit": 1}).status_code == 200
        assert client.get("/v1/events", params={"limit": 200}).status_code == 200

    def test_limit_out_of_bounds_rejected(self, client: TestClient) -> None:
        assert client.get("/v1/events", params={"limit": 0}).status_code == 422
        assert client.get("/v1/events", params={"limit": 201}).status_code == 422

    def test_negative_offset_rejected(self, client: TestClient) -> None:
        assert client.get("/v1/events", params={"offset": -1}).status_code == 422

    def test_invalid_severity_rejected(self, client: TestClient) -> None:
        assert client.get("/v1/events", params={"severity": "URGENT"}).status_code == 422

    def test_invalid_time_rejected(self, client: TestClient) -> None:
        assert (
            client.get("/v1/events", params={"start_time": "not-a-time"}).status_code
            == 422
        )

    def test_empty_table(self, client: TestClient) -> None:
        response = client.get("/v1/events")
        assert response.status_code == 200
        assert response.json() == {"items": [], "total": 0}


class TestEventStats:
    def test_empty_table(self, client: TestClient) -> None:
        response = client.get("/v1/events/stats")
        assert response.status_code == 200
        assert response.json() == {
            "total": 0,
            "by_severity": {},
            "by_event_type": {},
            "avg_value": None,
            "min_value": None,
            "max_value": None,
        }

    def test_overall_aggregates(self, client: TestClient, db_session: Session) -> None:
        _seed_events(db_session)
        response = client.get("/v1/events/stats")
        body = response.json()
        assert body["total"] == 4
        assert body["by_severity"] == {"LOW": 1, "HIGH": 2, "CRITICAL": 1}
        assert body["by_event_type"] == {"CPU_SPIKE": 2, "MEM_SPIKE": 2}
        assert body["avg_value"] == 25.0
        assert body["min_value"] == 10.0
        assert body["max_value"] == 40.0

    def test_with_filters(self, client: TestClient, db_session: Session) -> None:
        _seed_events(db_session)
        response = client.get("/v1/events/stats", params={"source": "server-02"})
        body = response.json()
        assert body["total"] == 2
        assert body["by_severity"] == {"HIGH": 1, "CRITICAL": 1}
        assert body["by_event_type"] == {"MEM_SPIKE": 2}
        assert body["avg_value"] == 35.0
        assert body["min_value"] == 30.0
        assert body["max_value"] == 40.0

    def test_filtered_to_no_rows(self, client: TestClient, db_session: Session) -> None:
        _seed_events(db_session)
        response = client.get("/v1/events/stats", params={"source": "server-99"})
        assert response.json() == {
            "total": 0,
            "by_severity": {},
            "by_event_type": {},
            "avg_value": None,
            "min_value": None,
            "max_value": None,
        }
