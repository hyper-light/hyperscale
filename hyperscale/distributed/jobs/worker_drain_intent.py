"""Worker dispatch drain intent state."""

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class WorkerDrainIntent:
    """Control-plane intent preventing new workflow dispatch to a worker."""

    worker_id: str
    epoch: int
    reason: str
    started_at: float = field(default_factory=time.monotonic)
