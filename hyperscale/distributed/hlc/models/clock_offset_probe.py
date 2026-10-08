from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClockOffsetProbe(Message):
    """A request for a peer's physical time (AD-39 offset measurement)."""

    prober_id: str = ""
