"""``LoadShedderConfig`` -- pickled under the namespace
``hyperscale.distributed.reliability.load_shedding`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from hyperscale.distributed.reliability.overload import OverloadState
from hyperscale.distributed.reliability.priority import RequestPriority

if TYPE_CHECKING:
    from .load_shedder import LoadShedder


@dataclass(slots=True)
class LoadShedderConfig:
    """Configuration for LoadShedder behavior."""

    # Mapping of overload state to minimum priority that gets shed
    # Requests with priority >= this threshold are shed
    shed_thresholds: dict[OverloadState, RequestPriority | None] = field(
        default_factory=lambda: {
            OverloadState.HEALTHY: None,  # Accept all
            OverloadState.BUSY: RequestPriority.LOW,  # Shed TELEMETRY only
            OverloadState.STRESSED: RequestPriority.NORMAL,  # Shed DATA and TELEMETRY
            OverloadState.OVERLOADED: RequestPriority.HIGH,  # Shed all except CONTROL
        }
    )
