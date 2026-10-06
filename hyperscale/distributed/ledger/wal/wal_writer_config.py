"""``WALWriterConfig`` -- pickled under the namespace
``hyperscale.distributed.ledger.wal.wal_writer`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class WALWriterConfig:
    batch_max_entries: int = 1000
    batch_max_bytes: int = 1024 * 1024
    queue_max_size: int = 10000
    overflow_size: int = 1000
    throttle_threshold: float = 0.70
    batch_threshold: float = 0.85
    reject_threshold: float = 0.95
