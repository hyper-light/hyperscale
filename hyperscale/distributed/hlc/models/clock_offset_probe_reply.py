from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClockOffsetProbeReply(Message):
    """A peer's physical time, in Unix milliseconds, read while answering
    a ``ClockOffsetProbe``."""

    responder_id: str = ""
    responder_physical_ms: int = 0
