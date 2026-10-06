"""``Checkpoint`` -- pickled under the namespace
``hyperscale.distributed.ledger.checkpoint.checkpoint`` (see that module)."""

from __future__ import annotations

from typing import Any
import msgspec
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp


class Checkpoint(msgspec.Struct, frozen=True):
    local_lsn: int
    regional_lsn: int
    global_lsn: int
    hlc: HLCTimestamp
    job_states: dict[str, dict[str, Any]]
    created_at_ms: int
    # The ledger's next fence token. Compaction drops the JOB_CREATED
    # entries replay used to advance it from, so without this a restart
    # re-issued fence tokens already held by live jobs. 0 = written
    # before this field existed (recovery then seeds from job states).
    next_fence_token: int = 0
