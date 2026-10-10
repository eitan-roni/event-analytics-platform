from app.messaging.publisher import EventPublisher
from app.schemas.event import EventAccepted, EventCreate


class EventIngestionService:
    def __init__(self, publisher: EventPublisher) -> None:
        self._publisher = publisher

    def ingest(self, event: EventCreate) -> EventAccepted:
        # Keyed by source so that events from one source keep their order
        # within a partition.
        self._publisher.publish(
            key=event.source, value=event.model_dump_json().encode("utf-8")
        )
        return EventAccepted(event_id=event.event_id)
