class DomainError(Exception):
    """Base domain error."""


class DuplicateEventError(DomainError):
    def __init__(self, event_id: str) -> None:
        super().__init__(f"duplicate event: {event_id}")
        self.event_id = event_id


class RateLimitedError(DomainError):
    def __init__(self, tenant_id: str, retry_after_ms: int) -> None:
        super().__init__(f"rate limited tenant={tenant_id}")
        self.tenant_id = tenant_id
        self.retry_after_ms = retry_after_ms


class ValidationError(DomainError):
    pass
