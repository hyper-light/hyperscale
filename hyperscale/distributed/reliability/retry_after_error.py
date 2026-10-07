"""``RetryAfterError`` -- a refusal that names when to try again."""


class RetryAfterError(Exception):
    """A refusal whose sender said when the operation may succeed:
    ``retry_after_seconds`` from now. ``RetryExecutor`` waits at least that
    long before retrying it -- an earlier attempt would only be refused
    again."""

    def __init__(self, message: str, retry_after_seconds: float) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds
