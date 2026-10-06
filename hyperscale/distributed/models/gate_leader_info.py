"""Wire model ``GateLeaderInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class GateLeaderInfo:
    """
    Information about a gate acting as job leader for a specific job (Section 9.1.1).

    Used by clients to track which gate is the authoritative source
    for a job's status and control operations.
    """

    gate_addr: tuple[str, int]  # (host, port) of the gate
    fence_token: int  # Fencing token for ordering
    last_updated: float  # time.monotonic() when last updated
