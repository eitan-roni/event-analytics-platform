from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.repositories.event_repository import EventRepository
from app.schemas.event import EventAccepted, EventCreate
from app.services.event_service import EventService

router = APIRouter(prefix="/v1", tags=["events"])


@router.post("/events", response_model=EventAccepted, status_code=status.HTTP_201_CREATED)
def create_event(event: EventCreate, db: Session = Depends(get_db)) -> EventAccepted:
    service = EventService(EventRepository(db))
    return service.ingest(event)
