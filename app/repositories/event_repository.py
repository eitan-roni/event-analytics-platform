from sqlalchemy import Select, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models.event import EventORM
from app.schemas.event import EventFilters


class EventRepository:
    def __init__(self, db: Session) -> None:
        self._db = db

    def insert_if_absent(self, event: EventORM) -> bool:
        """Insert and commit; returns False if the event_id already exists.

        ON CONFLICT DO NOTHING makes redelivered messages harmless.
        """
        stmt = (
            insert(EventORM)
            .values(
                event_id=event.event_id,
                source=event.source,
                event_type=event.event_type,
                severity=event.severity,
                timestamp=event.timestamp,
                value=event.value,
            )
            .on_conflict_do_nothing(index_elements=[EventORM.event_id])
            # RETURNING yields no row on conflict; rowcount isn't reliable
            # for ORM-enabled inserts executed through a Session.
            .returning(EventORM.id)
        )
        inserted_id = self._db.execute(stmt).scalar_one_or_none()
        self._db.commit()
        return inserted_id is not None

    def list_events(self, filters: EventFilters, limit: int, offset: int) -> list[EventORM]:
        stmt = self._apply_filters(select(EventORM), filters)
        stmt = stmt.order_by(EventORM.timestamp.desc(), EventORM.id.desc())
        stmt = stmt.limit(limit).offset(offset)
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
