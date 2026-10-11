from collections.abc import Callable, Generator, Iterator
from contextlib import AbstractContextManager, contextmanager

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.api.events import get_event_publisher, get_stats_cache
from app.core.config import DATABASE_URL, TEST_DATABASE_URL
from app.core.exceptions import CacheError, PublishError
from app.db.base import Base
from app.db.session import get_db
from app.main import app
from app.models.event import EventORM


class InMemoryEventPublisher:
    """EventPublisher fake that records messages instead of sending them."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes]] = []
        self.fail = False

    def publish(self, key: str, value: bytes) -> None:
        if self.fail:
            raise PublishError("simulated publish failure")
        self.messages.append((key, value))

    def close(self) -> None:
        pass


class InMemoryCache:
    """Cache fake with TTL expiry against a fake clock.

    Advance ``now`` (seconds) to simulate time passing instead of sleeping.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self._entries: dict[str, tuple[str, float]] = {}
        self.set_calls: list[tuple[str, str, int]] = []
        self.fail_get = False
        self.fail_set = False

    def get(self, key: str) -> str | None:
        if self.fail_get:
            raise CacheError("simulated cache read failure")
        entry = self._entries.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if self.now >= expires_at:
            del self._entries[key]
            return None
        return value

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        if self.fail_set:
            raise CacheError("simulated cache write failure")
        self.set_calls.append((key, value, ttl_seconds))
        self._entries[key] = (value, self.now + ttl_seconds)

    def put_raw(self, key: str, value: str) -> None:
        """Store a value directly (e.g. a corrupt entry), without expiry."""
        self._entries[key] = (value, float("inf"))


class InMemoryDeadLetterPublisher:
    """DeadLetterPublisher fake that records messages instead of sending them."""

    def __init__(self, call_log: list[str] | None = None) -> None:
        self.messages: list[tuple[bytes | None, bytes | None, list[tuple[str, bytes]]]] = []
        self.fail = False
        # optional log shared with other fakes to check call ordering
        self._call_log = call_log if call_log is not None else []

    def publish(
        self, key: bytes | None, value: bytes | None, headers: list[tuple[str, bytes]]
    ) -> None:
        if self.fail:
            raise PublishError("simulated dead-letter publish failure")
        self._call_log.append("dead_letter")
        self.messages.append((key, value, headers))

    def close(self) -> None:
        pass


def _assert_safe_test_database(url: str) -> None:
    if url == DATABASE_URL:
        raise RuntimeError(
            "TEST_DATABASE_URL must not equal DATABASE_URL — refusing to run "
            "tests against the development database."
        )
    database_name = url.rsplit("/", 1)[-1]
    if "test" not in database_name.lower():
        raise RuntimeError(
            f"Refusing to run tests against {database_name!r}: the test "
            "database name must contain 'test'."
        )


_assert_safe_test_database(TEST_DATABASE_URL)

test_engine = create_engine(TEST_DATABASE_URL)
TestSessionLocal = sessionmaker(bind=test_engine, autoflush=False, autocommit=False)


def borrowed_session_factory(
    session: Session,
) -> Callable[[], AbstractContextManager[Session]]:
    """Session factory for process_message that hands out an existing session.

    process_message closes the session it opens; this yields the given one
    without closing it, so the caller (usually the db_session fixture) keeps
    ownership.
    """

    @contextmanager
    def factory() -> Iterator[Session]:
        yield session

    return factory


@pytest.fixture(scope="session", autouse=True)
def _test_database_schema() -> Generator[None, None, None]:
    Base.metadata.create_all(bind=test_engine)
    yield
    Base.metadata.drop_all(bind=test_engine)


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    session = TestSessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.query(EventORM).delete()
        session.commit()
        session.close()


@pytest.fixture
def fake_publisher() -> InMemoryEventPublisher:
    return InMemoryEventPublisher()


@pytest.fixture
def fake_cache() -> InMemoryCache:
    return InMemoryCache()


@pytest.fixture
def client(
    db_session: Session, fake_publisher: InMemoryEventPublisher, fake_cache: InMemoryCache
) -> Generator[TestClient, None, None]:
    def _override_get_db() -> Generator[Session, None, None]:
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_event_publisher] = lambda: fake_publisher
    app.dependency_overrides[get_stats_cache] = lambda: fake_cache
    # Not entered as a context manager: this deliberately skips the app's
    # lifespan, which runs create_all against DATABASE_URL (the dev DB) and
    # creates the real Kafka producer and Redis client. Schema setup for tests
    # is handled by _test_database_schema; publisher and cache are fakes.
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
