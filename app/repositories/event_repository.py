from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.exceptions import DuplicateEventError
from app.models.event import EventORM
from app.schemas.event import EventFilters


class EventRepository:
    def __init__(self, db: Session) -> None:
        self._db = db

    def create(self, event: EventORM) -> EventORM:
        self._db.add(event)
        try:
            self._db.commit()
        except IntegrityError as exc:
            self._db.rollback()
            raise DuplicateEventError(event.event_id) from exc
        self._db.refresh(event)
        return event

    def list(self, filters: EventFilters, limit: int, offset: int) -> list[EventORM]:
        stmt = self._apply_filters(select(EventORM), filters)
        stmt = stmt.order_by(EventORM.timestamp.desc()).limit(limit).offset(offset)
        return list(self._db.scalars(stmt).all())

    def count(self, filters: EventFilters) -> int:
        stmt = self._apply_filters(select(func.count(EventORM.id)), filters)
        return self._db.scalar(stmt) or 0

    def get_stats(
        self, filters: EventFilters
    ) -> tuple[int, float | None, float | None, float | None, dict[str, int], dict[str, int]]:
        overall_stmt = self._apply_filters(
            select(
                func.count(EventORM.id),
                func.avg(EventORM.value),
                func.min(EventORM.value),
                func.max(EventORM.value),
            ),
            filters,
        )
        total, avg_value, min_value, max_value = self._db.execute(overall_stmt).one()

        severity_stmt = self._apply_filters(
            select(EventORM.severity, func.count(EventORM.id)), filters
        ).group_by(EventORM.severity)
        by_severity = dict(self._db.execute(severity_stmt).all())

        event_type_stmt = self._apply_filters(
            select(EventORM.event_type, func.count(EventORM.id)), filters
        ).group_by(EventORM.event_type)
        by_event_type = dict(self._db.execute(event_type_stmt).all())

        return total, avg_value, min_value, max_value, by_severity, by_event_type

    @staticmethod
    def _apply_filters(stmt: Select, filters: EventFilters) -> Select:
        if filters.source is not None:
            stmt = stmt.where(EventORM.source == filters.source)
        if filters.event_type is not None:
            stmt = stmt.where(EventORM.event_type == filters.event_type)
        if filters.severity is not None:
            stmt = stmt.where(EventORM.severity == filters.severity)
        if filters.start_time is not None:
            stmt = stmt.where(EventORM.timestamp >= filters.start_time)
        if filters.end_time is not None:
            stmt = stmt.where(EventORM.timestamp <= filters.end_time)
        return stmt
