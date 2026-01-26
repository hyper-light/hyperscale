from enum import IntEnum


class DurabilityMode(IntEnum):
    """
    Durability levels for log writes.

    Controls when writes are considered durable:
    - NONE: No sync (testing only, data loss on any failure)
    - FLUSH: Buffer flush only (current behavior, data loss on OS crash)
    - FSYNC: Per-write fsync (safest, highest latency)
    - FSYNC_BATCH: Batched fsync (recommended for WAL - balance of safety/perf)

    Recommended usage:
    - Data Plane (stats): FLUSH (default, current behavior)
    - Control Plane (WAL): FSYNC_BATCH (durability + throughput)
    - Testing: NONE (maximum speed, no durability)
    """

    NONE = 0
    FLUSH = 1
    FSYNC = 2
    FSYNC_BATCH = 3
