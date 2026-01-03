"""
Consistent Hashing Ring for deterministic job-to-gate assignment.

This implementation provides:
- Deterministic mapping: same key always maps to same node (when node is present)
- Minimal redistribution: adding/removing nodes only affects keys near the change
- Virtual nodes: ensures even distribution across physical nodes
- Backup assignment: supports finding backup nodes for fault tolerance

Usage:
    ring = ConsistentHashRing(virtual_nodes=150)
    ring.add_node("gate-1:9000")
    ring.add_node("gate-2:9000")

    primary = ring.get_node("job-abc123")  # Deterministic assignment
    backup = ring.get_backup("job-abc123")  # Different from primary
"""

from __future__ import annotations

import bisect
import hashlib
import threading
from typing import Iterator


class ConsistentHashRing:
    """
    A consistent hashing ring for distributed node assignment.

    Uses virtual nodes (vnodes) to ensure even key distribution across
    physical nodes. Each physical node is mapped to multiple positions
    on the ring, reducing hotspots and improving balance.

    Thread-safe: all operations are protected by a read-write lock pattern.

    Attributes:
        virtual_nodes: Number of virtual nodes per physical node.
            Higher values = better distribution but more memory.
            Recommended: 100-200 for production clusters.
    """

    __slots__ = (
        "_ring",
        "_sorted_keys",
        "_nodes",
        "_vnodes",
        "_lock",
    )

    def __init__(self, virtual_nodes: int = 150) -> None:
        """
        Initialize the consistent hash ring.

        Args:
            virtual_nodes: Number of virtual nodes per physical node.
                Default 150 provides good distribution for up to ~100 nodes.
        """
        if virtual_nodes < 1:
            raise ValueError("virtual_nodes must be >= 1")

        self._ring: dict[int, str] = {}  # hash position -> node_id
        self._sorted_keys: list[int] = []  # sorted hash positions for binary search
        self._nodes: set[str] = set()  # physical node ids
        self._vnodes = virtual_nodes
        self._lock = threading.RLock()

    def _hash(self, key: str) -> int:
        """
        Compute hash position for a key.

        Uses MD5 for good distribution (cryptographic strength not needed).
        Returns a 32-bit integer for reasonable ring size.
        """
        digest = hashlib.md5(key.encode(), usedforsecurity=False).digest()
        # Use first 4 bytes as unsigned 32-bit integer
        return int.from_bytes(digest[:4], byteorder="big")

    def add_node(self, node_id: str) -> None:
        """
        Add a physical node to the ring.

        Creates `virtual_nodes` positions on the ring for this node.
        If the node already exists, this is a no-op.

        Args:
            node_id: Unique identifier for the node (e.g., "gate-1:9000")
        """
        with self._lock:
            if node_id in self._nodes:
                return

            self._nodes.add(node_id)

            for i in range(self._vnodes):
                vnode_key = f"{node_id}:vnode:{i}"
                hash_pos = self._hash(vnode_key)
                self._ring[hash_pos] = node_id

            # Rebuild sorted keys
            self._sorted_keys = sorted(self._ring.keys())

    def remove_node(self, node_id: str) -> None:
        """
        Remove a physical node from the ring.

        Removes all virtual node positions for this node.
        If the node doesn't exist, this is a no-op.

        Args:
            node_id: Unique identifier for the node to remove
        """
        with self._lock:
            if node_id not in self._nodes:
                return

            self._nodes.discard(node_id)

            for i in range(self._vnodes):
                vnode_key = f"{node_id}:vnode:{i}"
                hash_pos = self._hash(vnode_key)
                self._ring.pop(hash_pos, None)

            # Rebuild sorted keys
            self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, key: str) -> str | None:
        """
        Get the node responsible for a key.

        Finds the first node position clockwise from the key's hash.
        Returns None if the ring is empty.

        Args:
            key: The key to look up (e.g., job_id)

        Returns:
            The node_id responsible for this key, or None if ring is empty.
        """
        with self._lock:
            if not self._sorted_keys:
                return None

            hash_pos = self._hash(key)

            # Binary search for first position >= hash_pos
            idx = bisect.bisect_left(self._sorted_keys, hash_pos)

            # Wrap around if past the end
            if idx >= len(self._sorted_keys):
                idx = 0

            return self._ring[self._sorted_keys[idx]]

    def get_backup(self, key: str) -> str | None:
        """
        Get the backup node for a key.

        Returns the next distinct physical node after the primary.
        If there's only one physical node, returns None.

        Args:
            key: The key to look up (e.g., job_id)

        Returns:
            The backup node_id, or None if no backup available.
        """
        with self._lock:
            if len(self._nodes) < 2:
                return None

            primary = self.get_node(key)
            if primary is None:
                return None

            hash_pos = self._hash(key)
            idx = bisect.bisect_left(self._sorted_keys, hash_pos)

            # Wrap around if past the end
            if idx >= len(self._sorted_keys):
                idx = 0

            # Find next distinct physical node
            ring_size = len(self._sorted_keys)
            for offset in range(1, ring_size):
                check_idx = (idx + offset) % ring_size
                candidate = self._ring[self._sorted_keys[check_idx]]
                if candidate != primary:
                    return candidate

            # Should not reach here if len(nodes) >= 2
            return None

    def get_nodes_for_key(self, key: str, count: int = 2) -> list[str]:
        """
        Get multiple nodes for a key (for replication).

        Returns up to `count` distinct physical nodes, starting with
        the primary and proceeding clockwise around the ring.

        Args:
            key: The key to look up
            count: Maximum number of nodes to return

        Returns:
            List of node_ids, length is min(count, number of nodes)
        """
        with self._lock:
            if not self._sorted_keys:
                return []

            result: list[str] = []
            seen: set[str] = set()

            hash_pos = self._hash(key)
            idx = bisect.bisect_left(self._sorted_keys, hash_pos)

            ring_size = len(self._sorted_keys)
            for offset in range(ring_size):
                if len(result) >= count:
                    break

                check_idx = (idx + offset) % ring_size
                node = self._ring[self._sorted_keys[check_idx]]

                if node not in seen:
                    seen.add(node)
                    result.append(node)

            return result

    def get_all_nodes(self) -> list[str]:
        """
        Get all physical nodes in the ring.

        Returns:
            List of all node_ids (unordered)
        """
        with self._lock:
            return list(self._nodes)

    def __len__(self) -> int:
        """Return the number of physical nodes in the ring."""
        with self._lock:
            return len(self._nodes)

    def __contains__(self, node_id: str) -> bool:
        """Check if a node is in the ring."""
        with self._lock:
            return node_id in self._nodes

    def __iter__(self) -> Iterator[str]:
        """Iterate over physical nodes."""
        with self._lock:
            return iter(list(self._nodes))

    def key_distribution(self, sample_keys: list[str]) -> dict[str, int]:
        """
        Analyze key distribution across nodes.

        Useful for testing and debugging distribution quality.

        Args:
            sample_keys: List of keys to test

        Returns:
            Dict mapping node_id -> count of assigned keys
        """
        distribution: dict[str, int] = {node: 0 for node in self._nodes}

        for key in sample_keys:
            node = self.get_node(key)
            if node:
                distribution[node] += 1

        return distribution
