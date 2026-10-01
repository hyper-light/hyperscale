from __future__ import annotations

from typing import Callable

from hyperscale.distributed.hlc.clock_fence_verdict import ClockFenceVerdict
from hyperscale.distributed.hlc.clock_offset_bounds import ClockOffsetBounds
from hyperscale.distributed.hlc.hybrid_logical_clock import HybridLogicalClock
from hyperscale.distributed.runtime import Clock

# A node fences at 80% of the offset bound (CockroachDB's rule): it stops
# minting timestamps while its offset is still inside the bound peers
# enforce, so it leaves before they would start refusing it.
FENCE_FRACTION_OF_MAX_OFFSET = 0.8


class ClockOffsetMonitor:
    """Decides, from measured peer offsets, whether this node's clock is
    fenced (AD-39).

    Fenced when a quorum of the configured cluster measured this node's
    clock certainly more than the fence threshold away from theirs --
    this node counts itself as agreeing with itself, so in a cluster of
    two neither clock can fence the other -- or when the node's own HLC
    leads its physical clock by more than the offset bound (a merge never
    leads it further; only this node's own clock stepping back does, and
    peers would refuse what it then minted). Measurements
    older than ``sample_ttl_seconds`` no longer count.
    """

    __slots__ = (
        "_hlc",
        "_cluster_size",
        "_sample_ttl_seconds",
        "_clock",
        "_threshold_ms",
        "_bounds",
        "_verdict",
    )

    def __init__(
        self,
        hlc: HybridLogicalClock,
        cluster_size: Callable[[], int],
        sample_ttl_seconds: float,
        clock: Clock,
    ) -> None:
        if sample_ttl_seconds <= 0.0:
            raise ValueError(f"sample_ttl_seconds must be positive, got {sample_ttl_seconds}")
        self._hlc = hlc
        self._cluster_size = cluster_size
        self._sample_ttl_seconds = sample_ttl_seconds
        self._clock = clock
        self._threshold_ms = int(hlc.max_offset_ms * FENCE_FRACTION_OF_MAX_OFFSET)
        self._bounds: dict[str, ClockOffsetBounds] = {}
        # None until the first refresh: no evidence yet, so not fenced.
        self._verdict: ClockFenceVerdict | None = None

    @property
    def is_fenced(self) -> bool:
        return self._verdict is not None and self._verdict.fenced

    @property
    def verdict(self) -> ClockFenceVerdict | None:
        return self._verdict

    @property
    def measured_peers(self) -> frozenset[str]:
        return frozenset(self._bounds)

    def record(self, peer_id: str, bounds: ClockOffsetBounds) -> None:
        self._bounds[peer_id] = bounds

    def forget(self, peer_id: str) -> None:
        self._bounds.pop(peer_id, None)

    def refresh(self) -> ClockFenceVerdict:
        """Drop expired measurements and re-evaluate the fence."""
        oldest_counted = self._clock.monotonic() - self._sample_ttl_seconds
        self._bounds = {
            peer_id: bounds for peer_id, bounds in self._bounds.items() if bounds.measured_at >= oldest_counted
        }
        peers_beyond = sum(bounds.certainly_beyond(self._threshold_ms) for bounds in self._bounds.values())
        hlc_lead_ms = self._hlc.current.wall_ms - self._hlc.physical_ms()
        quorum = self._cluster_size() // 2 + 1
        self._verdict = ClockFenceVerdict(
            fenced=peers_beyond >= quorum or hlc_lead_ms > self._hlc.max_offset_ms,
            peers_beyond=peers_beyond,
            peers_measured=len(self._bounds),
            quorum=quorum,
            hlc_lead_ms=hlc_lead_ms,
            threshold_ms=self._threshold_ms,
        )
        return self._verdict
