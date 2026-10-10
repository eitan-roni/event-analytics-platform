from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from app.core.exceptions import PublishError
from app.db.session import get_db
from app.messaging.publisher import EventPublisher
from app.repositories.event_repository import EventRepository
from app.schemas.event import (
    EventAccepted,
    EventCreate,
    EventFilters,
    EventListResponse,
    EventStats,
    Severity,
)
from app.services.event_service import EventService
from app.services.ingestion_service import EventIngestionService

router = APIRouter(prefix="/v1", tags=["events"])


def get_event_publisher(request: Request) -> EventPublisher:
    return request.app.state.event_publisher


def get_event_filters(
    source: str | None = None,
    event_type: str | None = None,
    severity: Severity | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> EventFilters:
    return EventFilters(
        source=source,
        event_type=event_type,
        severity=severity.value if severity is not None else None,
        start_time=start_time,
        end_time=end_time,
    )


@router.post("/events", response_model=EventAccepted, status_code=status.HTTP_202_ACCEPTED)
def create_event(
    event: EventCreate, publisher: EventPublisher = Depends(get_event_publisher)
) -> EventAccepted:
    service = EventIngestionService(publisher)
    try:
        return service.ingest(event)
    except PublishError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event could not be queued for processing; retry later",
        ) from exc


@router.get("/events", response_model=EventListResponse)
def list_events(
    filters: EventFilters = Depends(get_event_filters),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
) -> EventListResponse:
    service = EventService(EventRepository(db))
    return service.list_events(filters, limit, offset)


@router.get("/events/stats", response_model=EventStats)
def get_event_stats(
    filters: EventFilters = Depends(get_event_filters),
    db: Session = Depends(get_db),
) -> EventStats:
    service = EventService(EventRepository(db))
    return service.get_stats(filters)
