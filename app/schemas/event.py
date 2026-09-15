from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel


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
