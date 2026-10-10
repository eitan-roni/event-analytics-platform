import math
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

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


def _replace_non_finite_floats(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: _replace_non_finite_floats(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_replace_non_finite_floats(item) for item in value]
    return value


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    # Same response as FastAPI's default handler, except that NaN/Infinity
    # inputs echoed back in the errors are rendered as strings; JSON cannot
    # represent them, and the default handler fails with a 500.
    errors = _replace_non_finite_floats(jsonable_encoder(exc.errors()))
    return JSONResponse(status_code=422, content={"detail": errors})


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}
