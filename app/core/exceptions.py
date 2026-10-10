class DuplicateEventError(Exception):
    """Raised when an event with an already-used event_id is ingested."""

    def __init__(self, event_id: str) -> None:
        self.event_id = event_id
        super().__init__(f"Event with event_id '{event_id}' already exists")
