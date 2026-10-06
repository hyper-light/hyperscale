"""``FailureSource`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.hierarchical_failure_detector`` (see that module)."""

from enum import Enum, auto


class FailureSource(Enum):
    """Source of a failure detection event."""

    GLOBAL = auto()  # From global timing wheel
    JOB = auto()  # From job-specific detection
