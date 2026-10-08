"""``CircuitState`` -- pickled under the namespace
``hyperscale.distributed.swim.core.error_handler`` (see that module)."""

from enum import Enum, auto


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = auto()  # Normal operation
    OPEN = auto()  # Failing, rejecting requests
    HALF_OPEN = auto()  # Testing if recovery succeeded
