"""``WheelEntry`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.timing_wheel`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from typing import Generic, TypeVar

from .timing_wheel_shared import NodeAddress

if TYPE_CHECKING:
    from ._entry import _Entry

# Type variable for wheel entries (kept for backward-compatibility
# generics in callers that still import WheelEntry[T])
T = TypeVar("T")


@dataclass(slots=True)
class WheelEntry(Generic[T]):
    """Backward-compatibility shim.

    The previous two-level-wheel implementation used this dataclass
    to thread state + expiration time through bucket data structures.
    The event-driven replacement no longer uses it internally
    (entries are tracked by ``_Entry`` instead), but the type is
    retained so existing imports / type hints continue to resolve.
    """

    node: NodeAddress
    state: T
    expiration_time: float
    epoch: int = 0
