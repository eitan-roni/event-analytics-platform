from collections.abc import Generator

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import DATABASE_URL

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# Separate small engine for readiness checks, with a short connect timeout
# (libpq's practical minimum is 2 s), so a hung database cannot stall /ready.
_readiness_engine = create_engine(
    DATABASE_URL, connect_args={"connect_timeout": 2}, pool_size=1, max_overflow=0
)


def ping_database(engine: Engine | None = None) -> None:
    """Raise if the database cannot run a trivial query."""
    with (engine or _readiness_engine).connect() as connection:
        connection.execute(text("SELECT 1"))


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
