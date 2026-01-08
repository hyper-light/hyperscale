"""
Adaptive peer selector using Power of Two Choices with EWMA.

Combines deterministic rendezvous hashing with load-aware selection
for optimal traffic distribution.
"""

import random
from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed_rewrite.discovery.selection.rendezvous_hash import (
    WeightedRendezvousHash,
)
from hyperscale.distributed_rewrite.discovery.selection.ewma_tracker import (
    EWMATracker,
    EWMAConfig,
)
from hyperscale.distributed_rewrite.discovery.models.peer_info import PeerInfo


@dataclass
class PowerOfTwoConfig:
    """Configuration for Power of Two Choices selection."""

    candidate_count: int = 2
    """
    Number of candidates to consider (k in "power of k choices").

    More candidates = better load balancing, but less cache locality.
    - 2: Classic "power of two" (good balance)
    - 3-4: Better load balancing for hot keys
    - 1: Degrades to pure rendezvous hash (no load awareness)
    """

    use_rendezvous_ranking: bool = True
    """
    If True, candidates are top-k from rendezvous hash.
    If False, candidates are randomly selected.

    Rendezvous ranking provides better cache locality and
    deterministic fallback ordering.
    """

    latency_threshold_ms: float = 100.0
    """
    If best EWMA latency is below this, skip load-aware selection.

    Avoids unnecessary overhead when all peers are healthy.
    """

    random_seed: int | None = None
    """Optional seed for random selection (for testing)."""


@dataclass
class SelectionResult:
    """Result of peer selection."""

    peer_id: str
    """Selected peer ID."""

    effective_latency_ms: float
    """Effective latency of selected peer."""

    was_load_balanced: bool
    """True if load-aware selection was used."""

    candidates_considered: int
    """Number of candidates that were considered."""


@dataclass
class AdaptiveEWMASelector:
    """
    Adaptive peer selector using Power of Two Choices.

    Combines:
    1. Weighted Rendezvous Hash for deterministic candidate ranking
    2. EWMA tracking for load-aware selection
    3. Power of Two Choices for optimal load distribution

    Algorithm:
    1. Get top-k candidates from rendezvous hash for the key
    2. Query EWMA tracker for each candidate's effective latency
    3. Select candidate with lowest effective latency

    This provides:
    - O(1) selection with excellent load distribution
    - Deterministic fallback ordering (from rendezvous)
    - Automatic avoidance of slow/failing peers
    - Graceful degradation under partial failures

    Usage:
        selector = AdaptiveEWMASelector()
        selector.add_peer("peer1", weight=1.0)
        selector.add_peer("peer2", weight=1.0)

        # Select best peer for a key
        result = selector.select("job-123")

        # Record latency feedback
        selector.record_success(result.peer_id, latency_ms=15.0)
    """

    power_of_two_config: PowerOfTwoConfig = field(default_factory=PowerOfTwoConfig)
    """Configuration for power of two selection."""

    ewma_config: EWMAConfig = field(default_factory=EWMAConfig)
    """Configuration for EWMA tracking."""

    _rendezvous: WeightedRendezvousHash = field(
        default_factory=WeightedRendezvousHash
    )
    """Rendezvous hash for candidate ranking."""

    _ewma: EWMATracker = field(init=False)
    """EWMA tracker for latency feedback."""

    _random: random.Random = field(init=False, repr=False)
    """Random number generator for random selection mode."""

    def __post_init__(self) -> None:
        """Initialize EWMA tracker and RNG."""
        self._ewma = EWMATracker(config=self.ewma_config)
        self._random = random.Random(self.power_of_two_config.random_seed)

    def add_peer(self, peer_id: str, weight: float = 1.0) -> None:
        """
        Add a peer to the selector.

        Args:
            peer_id: Unique peer identifier
            weight: Selection weight (higher = more traffic)
        """
        self._rendezvous.add_peer(peer_id, weight)

    def add_peer_info(self, peer: PeerInfo) -> None:
        """
        Add a peer from PeerInfo.

        Uses peer.weight for selection weight.

        Args:
            peer: PeerInfo to add
        """
        self._rendezvous.add_peer(peer.peer_id, peer.weight)

    def remove_peer(self, peer_id: str) -> bool:
        """
        Remove a peer from the selector.

        Args:
            peer_id: The peer to remove

        Returns:
            True if removed
        """
        self._ewma.remove_peer(peer_id)
        return self._rendezvous.remove_peer(peer_id)

    def update_weight(self, peer_id: str, weight: float) -> bool:
        """
        Update a peer's selection weight.

        Args:
            peer_id: The peer to update
            weight: New weight

        Returns:
            True if updated
        """
        return self._rendezvous.update_weight(peer_id, weight)

    def select(self, key: str) -> SelectionResult | None:
        """
        Select the best peer for a key.

        Uses Power of Two Choices with EWMA for load-aware selection.

        Args:
            key: The key to select for (e.g., job_id)

        Returns:
            SelectionResult or None if no peers available
        """
        config = self.power_of_two_config

        if self._rendezvous.peer_count == 0:
            return None

        # Get candidates
        if config.use_rendezvous_ranking:
            candidates = self._rendezvous.select_n(key, config.candidate_count)
        else:
            # Random selection mode
            all_peers = self._rendezvous.peer_ids
            sample_size = min(config.candidate_count, len(all_peers))
            candidates = self._random.sample(all_peers, sample_size)

        if not candidates:
            return None

        # Single candidate = no load balancing needed
        if len(candidates) == 1:
            latency = self._ewma.get_effective_latency(candidates[0])
            return SelectionResult(
                peer_id=candidates[0],
                effective_latency_ms=latency,
                was_load_balanced=False,
                candidates_considered=1,
            )

        # Find best candidate by effective latency
        best_peer: str | None = None
        best_latency = float("inf")

        for peer_id in candidates:
            latency = self._ewma.get_effective_latency(peer_id)
            if latency < best_latency:
                best_latency = latency
                best_peer = peer_id

        # Check if load balancing was actually needed
        primary_latency = self._ewma.get_effective_latency(candidates[0])
        was_load_balanced = (
            best_peer != candidates[0]
            or primary_latency > config.latency_threshold_ms
        )

        return SelectionResult(
            peer_id=best_peer,  # type: ignore  # best_peer is guaranteed non-None
            effective_latency_ms=best_latency,
            was_load_balanced=was_load_balanced,
            candidates_considered=len(candidates),
        )

    def select_with_filter(
        self,
        key: str,
        filter_fn: Callable[[str], bool],
    ) -> SelectionResult | None:
        """
        Select best peer with a filter function.

        Args:
            key: The key to select for
            filter_fn: Function that returns True for acceptable peers

        Returns:
            SelectionResult or None if no acceptable peers
        """
        config = self.power_of_two_config

        if self._rendezvous.peer_count == 0:
            return None

        # Get more candidates than needed to account for filtering
        candidates = self._rendezvous.select_n(
            key, config.candidate_count * 3
        )

        # Filter candidates
        filtered = [p for p in candidates if filter_fn(p)]

        if not filtered:
            return None

        # Limit to configured count
        candidates = filtered[: config.candidate_count]

        # Find best by latency
        best_peer: str | None = None
        best_latency = float("inf")

        for peer_id in candidates:
            latency = self._ewma.get_effective_latency(peer_id)
            if latency < best_latency:
                best_latency = latency
                best_peer = peer_id

        return SelectionResult(
            peer_id=best_peer,  # type: ignore
            effective_latency_ms=best_latency,
            was_load_balanced=len(candidates) > 1,
            candidates_considered=len(candidates),
        )

    def record_success(self, peer_id: str, latency_ms: float) -> None:
        """
        Record a successful request.

        Args:
            peer_id: The peer that handled the request
            latency_ms: Request latency in milliseconds
        """
        self._ewma.record_success(peer_id, latency_ms)

    def record_failure(self, peer_id: str) -> None:
        """
        Record a failed request.

        Args:
            peer_id: The peer that failed
        """
        self._ewma.record_failure(peer_id)

    def get_effective_latency(self, peer_id: str) -> float:
        """
        Get effective latency for a peer.

        Args:
            peer_id: The peer to look up

        Returns:
            Effective latency in milliseconds
        """
        return self._ewma.get_effective_latency(peer_id)

    def get_ranked_peers(self, key: str, count: int) -> list[tuple[str, float]]:
        """
        Get ranked peers for a key with their effective latencies.

        Args:
            key: The key to rank for
            count: Number of peers to return

        Returns:
            List of (peer_id, effective_latency_ms) sorted by latency
        """
        candidates = self._rendezvous.select_n(key, count)
        ranked = [
            (peer_id, self._ewma.get_effective_latency(peer_id))
            for peer_id in candidates
        ]
        ranked.sort(key=lambda x: x[1])
        return ranked

    def decay_failures(self) -> int:
        """
        Decay failure counts for all peers.

        Call periodically to allow failed peers to recover.

        Returns:
            Number of peers with decayed failure counts
        """
        return self._ewma.decay_failure_counts()

    def clear(self) -> None:
        """Clear all peers and statistics."""
        self._rendezvous.clear()
        self._ewma.clear()

    @property
    def peer_count(self) -> int:
        """Return the number of peers."""
        return self._rendezvous.peer_count

    @property
    def peer_ids(self) -> list[str]:
        """Return all peer IDs."""
        return self._rendezvous.peer_ids

    def contains(self, peer_id: str) -> bool:
        """Check if a peer is in the selector."""
        return self._rendezvous.contains(peer_id)
