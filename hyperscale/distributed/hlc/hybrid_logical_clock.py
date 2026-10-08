from __future__ import annotations

from hyperscale.distributed.hlc.clock_offset_exceeded_error import ClockOffsetExceededError
from hyperscale.distributed.hlc.hlc_timestamp import (
    MAX_LOGICAL,
    MAX_NODE_ID,
    MAX_WALL_MS,
    HLCTimestamp,
)
from hyperscale.distributed.runtime import Clock


class HybridLogicalClock:
    """A hybrid logical clock with a bounded-offset invariant (AD-39;
    Kulkarni et al., "Logical Physical Clocks", 2014).

    Timestamps order physical time first, so they are causally
    consistent (an event that causally follows another gets a greater
    timestamp) and stay within ``max_offset_ms`` of real time: a remote
    timestamp further ahead of this node's physical clock than that is
    refused (``ClockOffsetExceededError``) rather than adopted, so one
    fast clock cannot drag the cluster's timestamps away from real time.

    Operations are synchronous: each is a read-modify-write with no
    await, so it is atomic on the event loop.
    """

    __slots__ = ("_node_id", "_clock", "_max_offset_ms", "_wall_ms", "_logical")

    def __init__(self, node_id: int, clock: Clock, max_offset_ms: int) -> None:
        if not 0 <= node_id <= MAX_NODE_ID:
            raise ValueError(f"node_id must be in [0, {MAX_NODE_ID}], got {node_id}")
        if max_offset_ms <= 0:
            raise ValueError(f"max_offset_ms must be positive, got {max_offset_ms}")
        self._node_id = node_id
        self._clock = clock
        self._max_offset_ms = max_offset_ms
        self._wall_ms = 0
        self._logical = 0

    @property
    def node_id(self) -> int:
        return self._node_id

    @property
    def max_offset_ms(self) -> int:
        return self._max_offset_ms

    @property
    def current(self) -> HLCTimestamp:
        """The latest timestamp issued or observed (does not advance)."""
        return HLCTimestamp(self._wall_ms, self._logical, self._node_id)

    def physical_ms(self) -> int:
        """This node's physical clock, in Unix milliseconds."""
        return min(int(self._clock.time() * 1000), MAX_WALL_MS)

    def now(self) -> HLCTimestamp:
        """Timestamp a local or send event."""
        physical = self.physical_ms()
        if physical > self._wall_ms:
            self._wall_ms, self._logical = physical, 0
        else:
            self._set(self._wall_ms, self._logical + 1)
        return self.current

    def check(self, remote: HLCTimestamp) -> None:
        """Refuse a remote timestamp beyond the offset bound, without
        merging it -- for validating input before it is accepted."""
        physical = self.physical_ms()
        if remote.wall_ms - physical > self._max_offset_ms:
            raise ClockOffsetExceededError(remote.wall_ms, physical, self._max_offset_ms)

    def receive(self, remote: HLCTimestamp) -> HLCTimestamp:
        """Merge a remote timestamp (a receive event) and timestamp it."""
        self.check(remote)
        physical = self.physical_ms()
        wall_ms = max(self._wall_ms, remote.wall_ms, physical)
        if wall_ms == self._wall_ms == remote.wall_ms:
            logical = max(self._logical, remote.logical) + 1
        elif wall_ms == self._wall_ms:
            logical = self._logical + 1
        elif wall_ms == remote.wall_ms:
            logical = remote.logical + 1
        else:
            logical = 0
        self._set(wall_ms, logical)
        return self.current

    def witness(self, own_history: HLCTimestamp) -> None:
        """Resume after this node's own recorded history (recovery):
        never issue a timestamp at or below one already persisted, even
        if the physical clock went backwards across the restart. No
        offset bound applies -- it is this node's own past."""
        if (own_history.wall_ms, own_history.logical) > (self._wall_ms, self._logical):
            self._wall_ms, self._logical = own_history.wall_ms, own_history.logical

    def _set(self, wall_ms: int, logical: int) -> None:
        # The logical counter carries into the physical component when
        # more events share a millisecond than it can count; that borrow
        # is bounded by the event rate and still subject to ``check``.
        if logical > MAX_LOGICAL:
            wall_ms, logical = wall_ms + 1, 0
        self._wall_ms, self._logical = wall_ms, logical
