from sqlalchemy.orm import Session

from app.models.event import EventORM


class EventRepository:
    def __init__(self, db: Session) -> None:
        self._db = db

    def create(self, event: EventORM) -> EventORM:
        self._db.add(event)
        self._db.commit()
        self._db.refresh(event)
        return event
