import logging
from enum import Enum

from pydantic import ValidationError

from app.schemas.event import EventCreate
from app.services.event_service import EventService

logger = logging.getLogger(__name__)


class MessageOutcome(str, Enum):
    STORED = "stored"
    DUPLICATE = "duplicate"
    SKIPPED = "skipped"


def handle_message(raw: bytes | None, service: EventService) -> MessageOutcome:
    """Parse, validate and persist one message value.

    Malformed or invalid messages are logged and skipped. Database errors
    propagate so the caller does not commit the offset.
    """
    if raw is None:
        logger.warning("Skipping message with no value")
        return MessageOutcome.SKIPPED

    try:
        event = EventCreate.model_validate_json(raw)
    except ValidationError as exc:
        logger.warning("Skipping invalid message: %s", exc.errors(include_input=False))
        return MessageOutcome.SKIPPED

    if service.store(event):
        return MessageOutcome.STORED
    logger.info("Event %s already stored; ignoring duplicate", event.event_id)
    return MessageOutcome.DUPLICATE
