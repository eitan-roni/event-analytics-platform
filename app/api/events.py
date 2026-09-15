from fastapi import APIRouter, status

from app.schemas.event import EventAccepted, EventCreate
from app.services.event_service import EventService

router = APIRouter(prefix="/v1", tags=["events"])
event_service = EventService()


@router.post("/events", response_model=EventAccepted, status_code=status.HTTP_201_CREATED)
def create_event(event: EventCreate) -> EventAccepted:
    return event_service.ingest(event)
