from dataclasses import dataclass
from enum import Enum

from pydantic import ValidationError
from sqlalchemy.exc import DataError, IntegrityError

from app.schemas.event import EventCreate
from app.services.event_service import EventService


class MessageOutcome(str, Enum):
    STORED = "stored"
    DUPLICATE = "duplicate"
    # cannot be processed, ever; goes to the dead-letter topic
    REJECTED = "rejected"


class RejectionCategory(str, Enum):
    INVALID_PAYLOAD = "invalid_payload"
    DATABASE_REJECTED = "database_rejected"


@dataclass(frozen=True)
class Rejection:
    category: RejectionCategory
    detail: str


@dataclass(frozen=True)
class HandlingResult:
    outcome: MessageOutcome
    rejection: Rejection | None = None


def _rejected(category: RejectionCategory, detail: str) -> HandlingResult:
    return HandlingResult(MessageOutcome.REJECTED, Rejection(category, detail))


def _describe_validation_error(exc: ValidationError) -> str:
    return "; ".join(
        f"{'.'.join(str(part) for part in error['loc']) or '<root>'}: {error['msg']}"
        for error in exc.errors(include_input=False)
    )


def handle_message(raw: bytes | None, service: EventService) -> HandlingResult:
    """Parse, validate and persist one message value.

    Messages that can never be processed are returned as REJECTED with the
    reason: invalid payloads, and DataError / IntegrityError, which are
    specific to the message and would fail on every redelivery. Any other
    database error (e.g. OperationalError) propagates so the caller does not
    commit the offset.
    """
    if raw is None:
        return _rejected(RejectionCategory.INVALID_PAYLOAD, "message has no value")

    try:
        event = EventCreate.model_validate_json(raw)
    except ValidationError as exc:
        return _rejected(RejectionCategory.INVALID_PAYLOAD, _describe_validation_error(exc))

    try:
        stored = service.store(event)
    except (DataError, IntegrityError) as exc:
        cause = exc.orig or exc
        return _rejected(
            RejectionCategory.DATABASE_REJECTED, f"{type(cause).__name__}: {cause}"
        )

    return HandlingResult(MessageOutcome.STORED if stored else MessageOutcome.DUPLICATE)
