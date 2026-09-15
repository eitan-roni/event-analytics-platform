from app.models.event import EventORM
from app.repositories.event_repository import EventRepository
from app.schemas.event import EventAccepted, EventCreate


class EventService:
    def __init__(self, repository: EventRepository) -> None:
        self._repository = repository

    def ingest(self, event: EventCreate) -> EventAccepted:
        event_orm = EventORM(
            event_id=event.event_id,
            source=event.source,
            event_type=event.event_type,
            severity=event.severity.value,
            timestamp=event.timestamp,
            value=event.value,
        )
        persisted = self._repository.create(event_orm)
        return EventAccepted(event_id=persisted.event_id)
