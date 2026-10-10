from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.events import router as events_router
from app.core.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_EVENTS_TOPIC
from app.db.base import Base
from app.db.session import engine
from app.messaging.publisher import KafkaEventPublisher


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    Base.metadata.create_all(bind=engine)
    publisher = KafkaEventPublisher(KAFKA_BOOTSTRAP_SERVERS, KAFKA_EVENTS_TOPIC)
    app.state.event_publisher = publisher
    try:
        yield
    finally:
        publisher.close()


app = FastAPI(title="Event Analytics Platform", lifespan=lifespan)
app.include_router(events_router)


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}
