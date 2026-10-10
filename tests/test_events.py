import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.event import EventORM
from tests.conftest import InMemoryEventPublisher

VALID_PAYLOAD = {
    "event_id": "evt-123",
    "source": "server-01",
    "event_type": "CPU_SPIKE",
    "severity": "HIGH",
    "timestamp": "2026-09-09T12:30:00Z",
    "value": 92.5,
}


def test_create_event_publishes_and_returns_202(
    client: TestClient, fake_publisher: InMemoryEventPublisher, db_session: Session
) -> None:
    response = client.post("/v1/events", json=VALID_PAYLOAD)
    assert response.status_code == 202
    assert response.json() == {"event_id": "evt-123", "status": "accepted"}

    assert len(fake_publisher.messages) == 1
    key, value = fake_publisher.messages[0]
    assert key == "server-01"
    assert json.loads(value) == {
        "event_id": "evt-123",
        "source": "server-01",
        "event_type": "CPU_SPIKE",
        "severity": "HIGH",
        "timestamp": "2026-09-09T12:30:00Z",
        "value": 92.5,
    }

    # persistence is the consumer's job; the API writes nothing
    assert db_session.query(EventORM).count() == 0


def test_create_event_publish_failure_returns_503(
    client: TestClient, fake_publisher: InMemoryEventPublisher
) -> None:
    fake_publisher.fail = True
    response = client.post("/v1/events", json=VALID_PAYLOAD)
    assert response.status_code == 503
    assert fake_publisher.messages == []


def test_create_event_missing_field(
    client: TestClient, fake_publisher: InMemoryEventPublisher
) -> None:
    payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "source"}
    response = client.post("/v1/events", json=payload)
    assert response.status_code == 422
    assert fake_publisher.messages == []


def test_create_event_invalid_severity(
    client: TestClient, fake_publisher: InMemoryEventPublisher
) -> None:
    payload = {**VALID_PAYLOAD, "severity": "URGENT"}
    response = client.post("/v1/events", json=payload)
    assert response.status_code == 422
    assert fake_publisher.messages == []


def test_create_event_invalid_timestamp(
    client: TestClient, fake_publisher: InMemoryEventPublisher
) -> None:
    payload = {**VALID_PAYLOAD, "timestamp": "not-a-timestamp"}
    response = client.post("/v1/events", json=payload)
    assert response.status_code == 422
    assert fake_publisher.messages == []


@pytest.mark.parametrize("field", ["event_id", "source", "event_type"])
@pytest.mark.parametrize(
    "bad_value",
    [pytest.param("", id="empty"), pytest.param("server\u000001", id="nul")],
)
def test_create_event_invalid_string_field(
    client: TestClient, fake_publisher: InMemoryEventPublisher, field: str, bad_value: str
) -> None:
    payload = {**VALID_PAYLOAD, field: bad_value}
    response = client.post("/v1/events", json=payload)
    assert response.status_code == 422
    assert fake_publisher.messages == []


@pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "-Infinity", "1e400"])
def test_create_event_non_finite_value(
    client: TestClient, fake_publisher: InMemoryEventPublisher, bad_value: str
) -> None:
    # Python's json module accepts these non-standard literals (1e400
    # overflows to inf), and httpx won't serialize them, so send raw bytes.
    body = json.dumps({**VALID_PAYLOAD, "value": 0}).replace('"value": 0', f'"value": {bad_value}')
    response = client.post(
        "/v1/events", content=body, headers={"content-type": "application/json"}
    )
    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "finite_number"
    assert fake_publisher.messages == []


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

    def test_pagination_is_deterministic_with_tied_timestamps(
        self, client: TestClient, db_session: Session
    ) -> None:
        tied_timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
        db_session.add_all(
            EventORM(
                event_id=f"tied-{i}",
                source="server-01",
                event_type="CPU_SPIKE",
                severity="LOW",
                timestamp=tied_timestamp,
                value=float(i),
            )
            for i in range(5)
        )
        db_session.commit()

        seen: list[str] = []
        for offset in range(5):
            response = client.get("/v1/events", params={"limit": 1, "offset": offset})
            assert response.status_code == 200
            items = response.json()["items"]
            assert len(items) == 1
            seen.append(items[0]["event_id"])

        # every event appears exactly once across the pages — no repeats,
        # no gaps — even though they all share one timestamp.
        assert sorted(seen) == sorted(f"tied-{i}" for i in range(5))
        assert len(set(seen)) == 5

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
