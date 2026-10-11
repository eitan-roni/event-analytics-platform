"""Prometheus metrics of the API process: definitions, request middleware, endpoint.

Labels are low-cardinality by design: the route template (never the raw
path) and fixed value sets; never event_id, source or other user input.
The default registry is per process, so with several uvicorn workers each
worker would expose its own counters.
"""

import time

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.types import ASGIApp, Message, Receive, Scope, Send

HTTP_REQUESTS = Counter(
    "http_requests_total",
    "HTTP requests handled, by method, route template and status code.",
    ["method", "route", "status"],
)
HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds, by method, route template and status code.",
    ["method", "route", "status"],
)
EVENTS_PUBLISHED = Counter(
    "events_published_total",
    "Events the API published to Kafka, by result (ok, error).",
    ["result"],
)
STATS_CACHE_LOOKUPS = Counter(
    "stats_cache_lookups_total",
    "Stats cache lookups, by result (hit, miss, error).",
    ["result"],
)

# Operational endpoints, kept out of the request metrics.
EXCLUDED_PATHS = frozenset({"/metrics", "/health", "/ready"})
UNMATCHED_ROUTE = "<unmatched>"


class RequestMetricsMiddleware:
    """Records request count and duration per method, route template and status."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] in EXCLUDED_PATHS:
            await self._app(scope, receive, send)
            return

        status = 500  # if the app raises before sending a response
        start = time.perf_counter()

        async def send_with_status(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self._app(scope, receive, send_with_status)
        finally:
            # The router stores the matched route in the shared scope.
            route = scope.get("route")
            labels = (
                scope["method"],
                getattr(route, "path", UNMATCHED_ROUTE),
                str(status),
            )
            HTTP_REQUESTS.labels(*labels).inc()
            HTTP_REQUEST_DURATION.labels(*labels).observe(time.perf_counter() - start)


router = APIRouter(tags=["operations"])


@router.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
