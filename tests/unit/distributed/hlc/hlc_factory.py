from hyperscale.distributed.env import Env
from hyperscale.distributed.hlc import HybridLogicalClock
from hyperscale.distributed.runtime import Clock, RealClock


def new_hybrid_logical_clock(node_id: int = 1, clock: Clock | None = None) -> HybridLogicalClock:
    """A node's HLC as production builds it: the Env offset bound over
    the given physical clock (real time by default)."""
    return HybridLogicalClock(
        node_id=node_id,
        clock=clock if clock is not None else RealClock(),
        max_offset_ms=Env().HLC_MAX_CLOCK_OFFSET_MS,
    )
