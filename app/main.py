import math
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api.events import router as events_router
from app.api.health import router as health_router
from app.api.metrics import RequestMetricsMiddleware
from app.api.metrics import router as metrics_router
from app.cache.cache import RedisCache
from app.core.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_EVENTS_TOPIC, REDIS_URL
from app.db.session import ping_database
from app.messaging.publisher import KafkaEventPublisher
from app.services.readiness_service import DependencyCheck, ReadinessService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    publisher = KafkaEventPublisher(KAFKA_BOOTSTRAP_SERVERS, KAFKA_EVENTS_TOPIC)
    app.state.event_publisher = publisher
    stats_cache = RedisCache(REDIS_URL)
    app.state.stats_cache = stats_cache
    app.state.readiness_service = ReadinessService(
        [
            DependencyCheck("postgres", critical=True, probe=ping_database),
            DependencyCheck("kafka", critical=True, probe=publisher.check_connection),
            # Not critical: the stats cache fails open to PostgreSQL.
            DependencyCheck("redis", critical=False, probe=stats_cache.ping),
        ]
    )
    try:
        yield
    finally:
        publisher.close()
        stats_cache.close()


app = FastAPI(title="Event Analytics Platform", lifespan=lifespan)
app.add_middleware(RequestMetricsMiddleware)
app.include_router(events_router)
app.include_router(health_router)
app.include_router(metrics_router)


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
