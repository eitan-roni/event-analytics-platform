import threading
import time
from collections.abc import Callable

import pytest
from fastapi.testclient import TestClient

from app.api.health import get_readiness_service
from app.cache.cache import RedisCache
from app.core.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_EVENTS_TOPIC, REDIS_URL
from app.db.session import ping_database
from app.main import app
from app.messaging.publisher import KafkaEventPublisher
from app.services.readiness_service import DependencyCheck, ReadinessService
from tests.conftest import test_engine


def _up() -> None:
    pass


def _down(message: str) -> Callable[[], None]:
    def probe() -> None:
        raise ConnectionError(message)

    return probe


def _service(
    postgres_up: bool = True, kafka_up: bool = True, redis_up: bool = True
) -> ReadinessService:
    return ReadinessService(
        [
            DependencyCheck("postgres", True, _up if postgres_up else _down("db unreachable")),
            DependencyCheck("kafka", True, _up if kafka_up else _down("broker unreachable")),
            DependencyCheck("redis", False, _up if redis_up else _down("redis unreachable")),
        ]
    )


def test_ready_when_all_dependencies_are_up() -> None:
    report = _service().check()

    assert report.ready is True
    assert [(s.name, s.up) for s in report.dependencies] == [
        ("postgres", True),
        ("kafka", True),
        ("redis", True),
    ]


@pytest.mark.parametrize("down", ["postgres", "kafka"])
def test_not_ready_when_a_critical_dependency_is_down(down: str) -> None:
    report = _service(**{f"{down}_up": False}).check()

    assert report.ready is False
    status = next(s for s in report.dependencies if s.name == down)
    assert status.up is False
    assert status.error is not None and status.error.startswith("ConnectionError: ")


def test_still_ready_when_only_redis_is_down() -> None:
    report = _service(redis_up=False).check()

    assert report.ready is True
    redis = next(s for s in report.dependencies if s.name == "redis")
    assert (redis.up, redis.critical) == (False, False)


def test_probe_still_running_at_the_deadline_is_reported_down() -> None:
    release = threading.Event()

    def hanging() -> None:
        release.wait(5)

    service = ReadinessService(
        [
            DependencyCheck("postgres", True, _up),
            DependencyCheck("redis", False, hanging),
        ],
        deadline_seconds=0.2,
    )
    try:
        start = time.perf_counter()
        report = service.check()
        elapsed = time.perf_counter() - start
    finally:
        release.set()

    assert elapsed < 1
    assert report.ready is True  # only the non-critical probe overran
    redis = next(s for s in report.dependencies if s.name == "redis")
    assert (redis.up, redis.error) == (False, "timed out after 0.2s")


def test_long_errors_are_truncated() -> None:
    service = ReadinessService([DependencyCheck("x", True, _down("e" * 1000))])

    (status,) = service.check().dependencies

    assert status.error is not None and len(status.error) == 200


@pytest.fixture
def ready_client(client: TestClient) -> Callable[[ReadinessService], TestClient]:
    # The client fixture clears dependency overrides afterwards.
    def with_service(service: ReadinessService) -> TestClient:
        app.dependency_overrides[get_readiness_service] = lambda: service
        return client

    return with_service


def test_ready_endpoint_returns_200_when_all_up(
    ready_client: Callable[[ReadinessService], TestClient],
) -> None:
    response = ready_client(_service()).get("/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "checks": {
            "postgres": {"status": "up", "critical": True},
            "kafka": {"status": "up", "critical": True},
            "redis": {"status": "up", "critical": False},
        },
    }


@pytest.mark.parametrize("down", ["postgres", "kafka"])
def test_ready_endpoint_returns_503_when_critical_dependency_down(
    ready_client: Callable[[ReadinessService], TestClient], down: str
) -> None:
    response = ready_client(_service(**{f"{down}_up": False})).get("/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"][down]["status"] == "down"
    assert body["checks"][down]["error"].startswith("ConnectionError: ")


def test_ready_endpoint_reports_redis_down_but_stays_ready(
    ready_client: Callable[[ReadinessService], TestClient],
) -> None:
    response = ready_client(_service(redis_up=False)).get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["checks"]["redis"] == {
        "status": "down",
        "critical": False,
        "error": "ConnectionError: redis unreachable",
    }


def test_health_does_not_call_dependencies(
    ready_client: Callable[[ReadinessService], TestClient],
) -> None:
    client = ready_client(_service(postgres_up=False, kafka_up=False, redis_up=False))

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_database_probe_against_real_postgres() -> None:
    # The test engine, never the dev database at DATABASE_URL.
    ping_database(test_engine)  # raises if PostgreSQL is unreachable


@pytest.mark.integration
def test_ready_with_real_dependencies(client: TestClient) -> None:
    publisher = KafkaEventPublisher(KAFKA_BOOTSTRAP_SERVERS, KAFKA_EVENTS_TOPIC)
    cache = RedisCache(REDIS_URL)
    service = ReadinessService(
        [
            DependencyCheck("postgres", True, lambda: ping_database(test_engine)),
            DependencyCheck("kafka", True, publisher.check_connection),
            DependencyCheck("redis", False, cache.ping),
        ]
    )
    app.dependency_overrides[get_readiness_service] = lambda: service
    try:
        response = client.get("/ready")
    finally:
        publisher.close()
        cache.close()

    assert response.status_code == 200, response.json()
    assert {name: c["status"] for name, c in response.json()["checks"].items()} == {
        "postgres": "up",
        "kafka": "up",
        "redis": "up",
    }
