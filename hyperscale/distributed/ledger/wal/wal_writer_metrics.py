"""``WALWriterMetrics`` -- pickled under the namespace
``hyperscale.distributed.ledger.wal.wal_writer`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class WALWriterMetrics:
    total_submitted: int = 0
    total_written: int = 0
    total_batches: int = 0
    total_bytes_written: int = 0
    total_fsyncs: int = 0
    total_rejected: int = 0
    total_overflow: int = 0
    total_errors: int = 0
    peak_queue_size: int = 0
    peak_batch_size: int = 0
