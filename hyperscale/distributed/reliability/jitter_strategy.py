"""``JitterStrategy`` -- pickled under the namespace
``hyperscale.distributed.reliability.retry`` (see that module)."""

from enum import Enum


class JitterStrategy(Enum):
    """
    Jitter strategies for retry delays.

    FULL: Maximum spread, best for independent clients
        delay = random(0, min(cap, base * 2^attempt))

    EQUAL: Guarantees minimum delay while spreading
        temp = min(cap, base * 2^attempt)
        delay = temp/2 + random(0, temp/2)

    DECORRELATED: Each retry depends on previous, good bounded growth
        delay = random(base, previous_delay * 3)

    NONE: No jitter, pure exponential backoff
        delay = min(cap, base * 2^attempt)
    """

    FULL = "full"
    EQUAL = "equal"
    DECORRELATED = "decorrelated"
    NONE = "none"
