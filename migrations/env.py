from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, create_engine

import app.models.event  # noqa: F401  (registers the models on Base.metadata)
from app.core.config import DATABASE_URL
from app.db.base import Base

config = context.config

# Skip logging setup when called programmatically (e.g. from the tests),
# so Alembic does not reconfigure the caller's loggers.
if config.config_file_name is not None and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Emit SQL to stdout instead of executing it (alembic upgrade --sql)."""
    context.configure(
        url=DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def _run_with_connection(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    # A caller may pass its own connection (the migration test does, to stay
    # on the test database); otherwise connect to DATABASE_URL.
    connection = config.attributes.get("connection")
    if connection is not None:
        _run_with_connection(connection)
        return

    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as connection:
            _run_with_connection(connection)
    finally:
        engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
