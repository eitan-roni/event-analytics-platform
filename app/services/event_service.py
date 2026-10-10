from app.models.event import EventORM
from app.repositories.event_repository import EventRepository
from app.schemas.event import (
    EventAccepted,
    EventCreate,
    EventFilters,
    EventListResponse,
    EventRead,
    EventStats,
)


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

    def list_events(self, filters: EventFilters, limit: int, offset: int) -> EventListResponse:
        items = self._repository.list_events(filters, limit, offset)
        total = self._repository.count(filters)
        return EventListResponse(
            items=[EventRead.model_validate(item) for item in items],
            total=total,
        )

    def get_stats(self, filters: EventFilters) -> EventStats:
        total, avg_value, min_value, max_value, by_severity, by_event_type = (
            self._repository.get_stats(filters)
        )
        return EventStats(
            total=total,
            by_severity=by_severity,
            by_event_type=by_event_type,
            avg_value=float(avg_value) if avg_value is not None else None,
            min_value=float(min_value) if min_value is not None else None,
            max_value=float(max_value) if max_value is not None else None,
        )
