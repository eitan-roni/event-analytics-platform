from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy.orm import Session

from app.api.metrics import UNMATCHED_ROUTE, RequestMetricsMiddleware
from app.models.event import EventORM
from tests.conftest import InMemoryCache, InMemoryEventPublisher
from tests.test_events import VALID_PAYLOAD


def _value(name: str, **labels: str) -> float:
    # Counters are process-global, so tests compare before/after deltas.
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _requests(method: str, route: str, status: str) -> float:
    return _value("http_requests_total", method=method, route=route, status=status)


def _durations(method: str, route: str, status: str) -> float:
    return _value("http_request_duration_seconds_count", method=method, route=route, status=status)


def test_request_is_counted_and_timed_by_route_template(client: TestClient) -> None:
    before_count = _requests("GET", "/v1/events", "200")
    before_duration = _durations("GET", "/v1/events", "200")

    assert client.get("/v1/events", params={"source": "server-01"}).status_code == 200

    assert _requests("GET", "/v1/events", "200") - before_count == 1
    assert _durations("GET", "/v1/events", "200") - before_duration == 1


def test_status_code_is_a_label(client: TestClient) -> None:
    before = _requests("POST", "/v1/events", "422")

    assert client.post("/v1/events", json={}).status_code == 422

    assert _requests("POST", "/v1/events", "422") - before == 1


def test_route_label_is_the_template_not_the_raw_path() -> None:
    # The project's routes have no path parameters, so use a minimal app with
    # the same middleware to show that the label is the template.
    app = FastAPI()
    app.add_middleware(RequestMetricsMiddleware)

    @app.get("/items/{item_id}")
    def get_item(item_id: str) -> dict[str, str]:
        return {"item_id": item_id}

    client = TestClient(app)
    before = _requests("GET", "/items/{item_id}", "200")

    assert client.get("/items/123").status_code == 200
    assert client.get("/items/abc").status_code == 200

    assert _requests("GET", "/items/{item_id}", "200") - before == 2
    assert _requests("GET", "/items/123", "200") == 0


def test_unmatched_path_uses_a_fixed_label(client: TestClient) -> None:
    before = _requests("GET", UNMATCHED_ROUTE, "404")

    assert client.get("/v1/nope/evt-secret-123").status_code == 404

    assert _requests("GET", UNMATCHED_ROUTE, "404") - before == 1
    assert _requests("GET", "/v1/nope/evt-secret-123", "404") == 0


def test_operational_endpoints_are_not_counted(client: TestClient) -> None:
    before = {
        path: sum(_requests("GET", path, status) for status in ("200", "503"))
        for path in ("/metrics", "/health")
    }

    client.get("/metrics")
    client.get("/health")

    for path in ("/metrics", "/health"):
        assert sum(_requests("GET", path, status) for status in ("200", "503")) == before[path]


def test_published_events_are_counted_by_result(
    client: TestClient, fake_publisher: InMemoryEventPublisher
) -> None:
    ok_before = _value("events_published_total", result="ok")
    error_before = _value("events_published_total", result="error")

    assert client.post("/v1/events", json=VALID_PAYLOAD).status_code == 202
    fake_publisher.fail = True
    assert client.post("/v1/events", json=VALID_PAYLOAD).status_code == 503

    assert _value("events_published_total", result="ok") - ok_before == 1
    assert _value("events_published_total", result="error") - error_before == 1


def test_invalid_event_is_not_counted_as_published(client: TestClient) -> None:
    before = sum(_value("events_published_total", result=r) for r in ("ok", "error"))

    assert client.post("/v1/events", json={}).status_code == 422

    assert sum(_value("events_published_total", result=r) for r in ("ok", "error")) == before


def test_stats_cache_lookups_are_counted_by_result(
    client: TestClient, db_session: Session, fake_cache: InMemoryCache
) -> None:
    db_session.add(
        EventORM(
            event_id="evt-m1",
            source="server-01",
            event_type="CPU_SPIKE",
            severity="LOW",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            value=1.0,
        )
    )
    db_session.commit()
    before = {r: _value("stats_cache_lookups_total", result=r) for r in ("hit", "miss", "error")}

    client.get("/v1/events/stats")  # miss
    client.get("/v1/events/stats")  # hit
    fake_cache.fail_get = True
    client.get("/v1/events/stats")  # error

    deltas = {
        r: _value("stats_cache_lookups_total", result=r) - before[r]
        for r in ("hit", "miss", "error")
    }
    assert deltas == {"hit": 1, "miss": 1, "error": 1}


def test_metrics_endpoint_returns_prometheus_text_format(client: TestClient) -> None:
    client.get("/v1/events")

    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"] == CONTENT_TYPE_LATEST
    families = {family.name: family for family in text_string_to_metric_families(response.text)}
    assert families["http_requests"].type == "counter"
    assert families["http_request_duration_seconds"].type == "histogram"
    assert "events_published" in families
    assert "stats_cache_lookups" in families
