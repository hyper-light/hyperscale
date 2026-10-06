"""``FailureEvent`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.hierarchical_failure_detector`` (see that module)."""

from dataclasses import dataclass, field

from .hierarchical_failure_detector_shared import _DEFAULT_CLOCK
from .hierarchical_failure_detector_shared import NodeAddress
from .hierarchical_failure_detector_shared import JobId
from .failure_source import FailureSource


@dataclass
class FailureEvent:
    """Event emitted when a node is declared dead."""

    node: NodeAddress
    source: FailureSource
    job_id: JobId | None  # Only set for JOB source
    incarnation: int
    timestamp: float = field(default_factory=lambda: _DEFAULT_CLOCK.monotonic())
