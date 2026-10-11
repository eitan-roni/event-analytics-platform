"""The Alembic migrations must produce exactly the schema the models define.

Runs in a throwaway schema of the test database, so it neither touches the
dev database nor interferes with the tables the rest of the suite uses.
"""

import uuid
from collections.abc import Generator
from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import Connection, inspect, text

from app.db.base import Base
from tests.conftest import test_engine

ALEMBIC_INI = Path(__file__).resolve().parent.parent / "alembic.ini"


@pytest.fixture
def empty_schema_connection() -> Generator[Connection, None, None]:
    # test_engine points at TEST_DATABASE_URL, which conftest has already
    # checked is not the dev database.
    schema = f"migration_check_{uuid.uuid4().hex[:12]}"
    with test_engine.connect() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        connection.execute(text(f'SET search_path TO "{schema}"'))
        connection.commit()
        try:
            yield connection
        finally:
            connection.rollback()
            connection.execute(text("SET search_path TO public"))
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            connection.commit()


def _alembic_config(connection: Connection) -> Config:
    config = Config(str(ALEMBIC_INI))
    config.attributes["connection"] = connection
    config.attributes["configure_logger"] = False
    return config


def test_migrations_match_models(empty_schema_connection: Connection) -> None:
    connection = empty_schema_connection
    config = _alembic_config(connection)

    command.upgrade(config, "head")
    connection.commit()

    context = MigrationContext.configure(
        connection, opts={"compare_type": True, "compare_server_default": True}
    )
    differences = compare_metadata(context, Base.metadata)
    assert differences == [], (
        "The models and the migrations disagree; create a migration with "
        f"`alembic revision --autogenerate`. Differences: {differences}"
    )


def test_migrations_downgrade_to_empty(empty_schema_connection: Connection) -> None:
    connection = empty_schema_connection
    config = _alembic_config(connection)
    command.upgrade(config, "head")

    command.downgrade(config, "base")
    connection.commit()

    assert inspect(connection).get_table_names() == ["alembic_version"]
