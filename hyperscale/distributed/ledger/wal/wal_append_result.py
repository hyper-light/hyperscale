"""``WALAppendResult`` -- pickled under the namespace
``hyperscale.distributed.ledger.wal.node_wal`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass
from hyperscale.distributed.reliability.robust_queue import QueuePutResult, QueueState
from hyperscale.distributed.reliability.backpressure import BackpressureLevel, BackpressureSignal

from .wal_entry import WALEntry


@dataclass(slots=True)
class WALAppendResult:
    entry: WALEntry
    queue_result: QueuePutResult

    @property
    def backpressure(self) -> BackpressureSignal:
        return self.queue_result.backpressure

    @property
    def backpressure_level(self) -> BackpressureLevel:
        return self.queue_result.backpressure.level

    @property
    def queue_state(self) -> QueueState:
        return self.queue_result.queue_state

    @property
    def in_overflow(self) -> bool:
        return self.queue_result.in_overflow
