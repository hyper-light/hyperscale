"""``WALBackpressureError`` -- pickled under the namespace
``hyperscale.distributed.ledger.wal.wal_writer`` (see that module)."""

from __future__ import annotations

from hyperscale.distributed.reliability.robust_queue import QueueState
from hyperscale.distributed.reliability.backpressure import BackpressureSignal


class WALBackpressureError(Exception):
    """Raised when WAL rejects a write due to backpressure."""

    def __init__(
        self,
        message: str,
        queue_state: QueueState,
        backpressure: BackpressureSignal,
    ) -> None:
        super().__init__(message)
        self.queue_state = queue_state
        self.backpressure = backpressure
