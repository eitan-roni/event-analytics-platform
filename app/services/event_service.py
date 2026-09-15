from app.schemas.event import EventAccepted, EventCreate


class EventService:
    def ingest(self, event: EventCreate) -> EventAccepted:
        return EventAccepted(event_id=event.event_id)
