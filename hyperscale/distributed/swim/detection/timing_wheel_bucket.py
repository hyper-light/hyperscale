"""``TimingWheelBucket`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.timing_wheel`` (see that module)."""

from .suspicion_state import SuspicionState
from .timing_wheel_shared import NodeAddress
from .wheel_entry import WheelEntry


class TimingWheelBucket:
    """Backward-compatibility shim.

    No longer used internally — entries live in ``TimingWheel._entries``
    directly. Retained because the symbol was part of the public
    detection module exports.
    """

    __slots__ = ("entries",)

    def __init__(self) -> None:
        self.entries: dict[NodeAddress, WheelEntry[SuspicionState]] = {}

    def __len__(self) -> int:
        return len(self.entries)
