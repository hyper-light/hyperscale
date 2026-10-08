"""``RetryAfterError`` -- a refusal that names when to try again."""

import copyreg


class RetryAfterError(Exception):
    """A refusal whose sender said when the operation may succeed:
    ``retry_after_seconds`` from now. ``RetryExecutor`` waits at least that
    long before retrying it -- an earlier attempt would only be refused
    again."""

    def __init__(self, message: str | None, retry_after_seconds: float) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds

    def __reduce__(self) -> tuple[object, tuple[type, ...], dict[str, object]]:
        """Pickle every subclass whole: ``Exception`` rebuilds an error by
        calling its class with ``args`` alone, which drops the hint (and a
        subclass's own fields) and fails outright for a constructor that
        requires them. Rebuild from the class, then restore ``args`` and the
        instance state."""
        return (copyreg.__newobj__, (type(self),), {**self.__dict__, "args": self.args})
