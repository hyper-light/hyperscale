"""Worker dispatch drain intent state."""

from dataclasses import dataclass, field

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()


@dataclass(slots=True)
class WorkerDrainIntent:
    """Control-plane intent preventing new workflow dispatch to a worker."""

    worker_id: str
    epoch: int
    reason: str
    started_at: float = field(default_factory=_DEFAULT_CLOCK.monotonic)
