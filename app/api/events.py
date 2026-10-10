from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.core.exceptions import DuplicateEventError
from app.db.session import get_db
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

router = APIRouter(prefix="/v1", tags=["events"])


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


@router.post("/events", response_model=EventAccepted, status_code=status.HTTP_201_CREATED)
def create_event(event: EventCreate, db: Session = Depends(get_db)) -> EventAccepted:
    service = EventService(EventRepository(db))
    try:
        return service.ingest(event)
    except DuplicateEventError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


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
