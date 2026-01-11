"""
Weighted Rendezvous Hash implementation for deterministic peer selection.

Provides consistent hashing with minimal reshuffling when peers are added or removed,
and supports weighted selection for capacity-aware distribution.
"""

import hashlib
import math
from dataclasses import dataclass, field


@dataclass
class WeightedRendezvousHash:
    """
    Weighted Rendezvous Hash (Highest Random Weight) implementation.

    Provides deterministic peer selection that:
    - Minimizes reshuffling when peers are added/removed
    - Supports weighted selection for capacity-aware distribution
    - Is consistent across all nodes for the same key

    The algorithm:
    1. For each peer, compute hash(key + peer_id)
    2. Apply weight transformation: score = -weight / ln(hash)
    3. Select peer with highest score

    This ensures:
    - Same key always maps to same peer (given same peer set)
    - Adding/removing peers only affects keys mapped to that peer
    - Higher weight peers get proportionally more keys

    Usage:
        hasher = WeightedRendezvousHash()
        hasher.add_peer("peer1", weight=1.0)
        hasher.add_peer("peer2", weight=2.0)  # Gets ~2x traffic

        # Get primary peer for a key
        peer = hasher.select("my-job-id")

        # Get ordered list (for fallback)
        ranked = hasher.select_n("my-job-id", n=3)
    """

    hash_seed: bytes = b"hyperscale-rendezvous"
    """Seed added to all hashes for domain separation."""

    _peers: dict[str, float] = field(default_factory=dict)
    """Map of peer_id to weight."""

    def add_peer(self, peer_id: str, weight: float = 1.0) -> None:
        """
        Add or update a peer with a weight.

        Args:
            peer_id: Unique identifier for the peer
            weight: Weight for selection (higher = more traffic). Must be > 0.

        Raises:
            ValueError: If weight is not positive
        """
        if weight <= 0:
            raise ValueError(f"Weight must be positive, got {weight}")
        self._peers[peer_id] = weight

    def remove_peer(self, peer_id: str) -> bool:
        """
        Remove a peer from the hash ring.

        Args:
            peer_id: The peer to remove

        Returns:
            True if peer was removed, False if not found
        """
        if peer_id in self._peers:
            del self._peers[peer_id]
            return True
        return False

    def update_weight(self, peer_id: str, weight: float) -> bool:
        """
        Update a peer's weight.

        Args:
            peer_id: The peer to update
            weight: New weight (must be > 0)

        Returns:
            True if peer was updated, False if not found

        Raises:
            ValueError: If weight is not positive
        """
        if weight <= 0:
            raise ValueError(f"Weight must be positive, got {weight}")
        if peer_id in self._peers:
            self._peers[peer_id] = weight
            return True
        return False

    def select(self, key: str) -> str | None:
        """
        Select the best peer for a key.

        Args:
            key: The key to hash (e.g., job_id, workflow_id)

        Returns:
            peer_id of the selected peer, or None if no peers
        """
        if not self._peers:
            return None

        best_peer: str | None = None
        best_score = float("-inf")

        for peer_id, weight in self._peers.items():
            score = self._compute_score(key, peer_id, weight)
            if score > best_score:
                best_score = score
                best_peer = peer_id

        return best_peer

    def select_n(self, key: str, n: int) -> list[str]:
        """
        Select the top N peers for a key in ranked order.

        Useful for getting fallback peers when primary is unavailable.

        Args:
            key: The key to hash
            n: Number of peers to return

        Returns:
            List of peer_ids, ordered by preference (best first)
        """
        if not self._peers:
            return []

        scored: list[tuple[float, str]] = []
        for peer_id, weight in self._peers.items():
            score = self._compute_score(key, peer_id, weight)
            scored.append((score, peer_id))

        # Sort by score descending (highest first)
        scored.sort(reverse=True)

        return [peer_id for _, peer_id in scored[:n]]

    def get_weight(self, peer_id: str) -> float | None:
        """
        Get a peer's current weight.

        Args:
            peer_id: The peer to look up

        Returns:
            Weight if peer exists, None otherwise
        """
        return self._peers.get(peer_id)

    def _compute_score(self, key: str, peer_id: str, weight: float) -> float:
        """
        Compute the rendezvous score for a key-peer combination.

        Uses the formula: score = -weight / ln(hash_normalized)

        Where hash_normalized is the hash output normalized to (0, 1).
        This ensures higher weights get proportionally higher scores.

        Args:
            key: The key being hashed
            peer_id: The peer identifier
            weight: The peer's weight

        Returns:
            Score value (higher is better)
        """
        # Compute combined hash
        combined = self.hash_seed + key.encode("utf-8") + peer_id.encode("utf-8")
        hash_bytes = hashlib.sha256(combined).digest()

        # Convert first 8 bytes to float in (0, 1)
        hash_int = int.from_bytes(hash_bytes[:8], "big")
        max_val = 2**64 - 1
        # Add small epsilon to avoid ln(0)
        hash_normalized = (hash_int / max_val) * 0.9999 + 0.0001

        # Apply weighted transformation
        # score = -weight / ln(hash)
        # Since ln(hash) is negative (hash < 1), this gives positive scores
        # Higher weight = higher score for same hash value
        return -weight / math.log(hash_normalized)

    def clear(self) -> int:
        """
        Remove all peers.

        Returns:
            Number of peers removed
        """
        count = len(self._peers)
        self._peers.clear()
        return count

    @property
    def peer_count(self) -> int:
        """Return the number of peers in the hash ring."""
        return len(self._peers)

    @property
    def peer_ids(self) -> list[str]:
        """Return list of all peer IDs."""
        return list(self._peers.keys())

    def contains(self, peer_id: str) -> bool:
        """Check if a peer is in the hash ring."""
        return peer_id in self._peers
