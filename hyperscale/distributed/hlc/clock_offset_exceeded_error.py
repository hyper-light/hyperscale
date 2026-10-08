from __future__ import annotations


class ClockOffsetExceededError(Exception):
    """A remote HLC timestamp is further ahead of this node's physical
    clock than the configured maximum offset allows (AD-39).

    Adopting it would drag this node's clock -- and every node it talks
    to -- beyond the bounded-drift invariant, so it is refused.
    """

    def __init__(self, remote_wall_ms: int, local_wall_ms: int, max_offset_ms: int) -> None:
        self.remote_wall_ms = remote_wall_ms
        self.local_wall_ms = local_wall_ms
        self.max_offset_ms = max_offset_ms
        super().__init__(
            f"remote HLC {remote_wall_ms}ms is {remote_wall_ms - local_wall_ms}ms ahead of "
            f"local physical time {local_wall_ms}ms (maximum offset {max_offset_ms}ms)"
        )
