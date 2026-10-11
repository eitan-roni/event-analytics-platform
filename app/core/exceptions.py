class PublishError(Exception):
    """Raised when an event could not be published to the message broker."""


class CacheError(Exception):
    """Raised when the cache cannot be read or written."""
