from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import DATABASE_URL, TEST_DATABASE_URL
from app.db.base import Base
from app.db.session import get_db
from app.main import app
from app.models.event import EventORM


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
def client(db_session: Session) -> Generator[TestClient, None, None]:
    def _override_get_db() -> Generator[Session, None, None]:
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    # Not entered as a context manager: this deliberately skips the app's
    # lifespan, which runs create_all against DATABASE_URL (the dev DB).
    # Schema setup for tests is handled by _test_database_schema instead.
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
