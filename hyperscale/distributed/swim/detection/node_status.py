"""``NodeStatus`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.hierarchical_failure_detector`` (see that module)."""

from enum import Enum, auto


class NodeStatus(Enum):
    """Status of a node from the perspective of failure detection."""

    ALIVE = auto()  # Not suspected at any layer
    SUSPECTED_GLOBAL = auto()  # Suspected at global layer (machine may be down)
    SUSPECTED_JOB = auto()  # Suspected for specific job(s) only
    DEAD_GLOBAL = auto()  # Declared dead at global layer
    DEAD_JOB = auto()  # Declared dead for specific job
