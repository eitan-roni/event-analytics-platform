from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StringConstraints


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


def _reject_nul(value: str) -> str:
    # PostgreSQL text columns cannot store NUL characters.
    if "\x00" in value:
        raise ValueError("must not contain NUL characters")
    return value


EventText = Annotated[str, StringConstraints(min_length=1), AfterValidator(_reject_nul)]


class EventCreate(BaseModel):
    event_id: EventText
    source: EventText
    event_type: EventText
    severity: Severity
    timestamp: datetime
    value: float = Field(allow_inf_nan=False)


class EventAccepted(BaseModel):
    event_id: str
    status: Literal["accepted"] = "accepted"


class EventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: str
    source: str
    event_type: str
    severity: str
    timestamp: datetime
    value: float


class EventListResponse(BaseModel):
    items: list[EventRead]
    total: int


class EventStats(BaseModel):
    total: int
    by_severity: dict[str, int]
    by_event_type: dict[str, int]
    avg_value: float | None
    min_value: float | None
    max_value: float | None


@dataclass
class EventFilters:
    """Query filters shared by the listing and stats endpoints.

    A plain value object (not a pydantic model) passed between the API,
    service, and repository layers.
    """

    source: str | None = None
    event_type: str | None = None
    severity: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
