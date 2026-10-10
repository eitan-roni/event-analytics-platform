from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class EventCreate(BaseModel):
    event_id: str
    source: str
    event_type: str
    severity: Severity
    timestamp: datetime
    value: float


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
