"""
Consistent Hash Ring - Per-job gate ownership calculation.

This class implements a consistent hashing ring for determining which gate
owns which job. It provides stable job-to-gate mapping that minimizes
remapping when gates join or leave the cluster.

Key properties:
- Consistent: Same job_id always maps to same gate (given same ring members)
- Balanced: Jobs are distributed roughly evenly across gates
- Minimal disruption: Adding/removing gates only remaps O(K/N) jobs
  where K is total jobs and N is number of gates

Uses virtual nodes (replicas) to improve distribution uniformity.
"""

import bisect
import hashlib
from dataclasses import dataclass, field


@dataclass(slots=True)
class HashRingNode:
    """A node in the consistent hash ring."""

    node_id: str
    tcp_host: str
    tcp_port: int
    weight: int = 1  # Relative weight for replica count


class ConsistentHashRing:
    """
    Consistent hash ring for job-to-gate mapping.

    Uses MD5 hashing with virtual nodes (replicas) to achieve
    uniform distribution of jobs across gates.

    Example usage:
        ring = ConsistentHashRing(replicas=150)

        # Add gates
        ring.add_node("gate-1", "10.0.0.1", 8080)
        ring.add_node("gate-2", "10.0.0.2", 8080)
        ring.add_node("gate-3", "10.0.0.3", 8080)

        # Find owner for a job
        owner = ring.get_node("job-12345")
        if owner:
            print(f"Job owned by {owner.node_id} at {owner.tcp_host}:{owner.tcp_port}")

        # Get multiple candidates for replication/failover
        candidates = ring.get_nodes("job-12345", count=2)
    """

    def __init__(self, replicas: int = 150):
        """
        Initialize ConsistentHashRing.

        Args:
            replicas: Number of virtual nodes per physical node.
                      Higher values provide better distribution but
                      use more memory. Default 150 is a good balance.
        """
        self._replicas = replicas

        # Sorted list of hash positions on the ring
        self._ring_positions: list[int] = []

        # Maps hash position -> node_id
        self._position_to_node: dict[int, str] = {}

        # Maps node_id -> HashRingNode
        self._nodes: dict[str, HashRingNode] = {}

    # =========================================================================
    # Node Management
    # =========================================================================

    def add_node(
        self,
        node_id: str,
        tcp_host: str,
        tcp_port: int,
        weight: int = 1,
    ) -> None:
        """
        Add a node to the hash ring.

        Args:
            node_id: Unique identifier for the node.
            tcp_host: TCP host address.
            tcp_port: TCP port.
            weight: Relative weight (higher = more jobs). Default 1.
        """
        if node_id in self._nodes:
            # Already exists, update it
            self.remove_node(node_id)

        node = HashRingNode(
            node_id=node_id,
            tcp_host=tcp_host,
            tcp_port=tcp_port,
            weight=weight,
        )
        self._nodes[node_id] = node

        # Add virtual nodes (replicas) to the ring
        replica_count = self._replicas * weight
        for replica_index in range(replica_count):
            key = f"{node_id}:{replica_index}"
            hash_value = self._hash(key)

            # Insert into sorted position list
            bisect.insort(self._ring_positions, hash_value)
            self._position_to_node[hash_value] = node_id

    def remove_node(self, node_id: str) -> HashRingNode | None:
        """
        Remove a node from the hash ring.

        Args:
            node_id: ID of node to remove.

        Returns:
            The removed node, or None if not found.
        """
        node = self._nodes.pop(node_id, None)
        if not node:
            return None

        # Remove all virtual nodes for this node
        replica_count = self._replicas * node.weight
        for replica_index in range(replica_count):
            key = f"{node_id}:{replica_index}"
            hash_value = self._hash(key)

            # Remove from position list
            try:
                self._ring_positions.remove(hash_value)
            except ValueError:
                pass  # Already removed

            self._position_to_node.pop(hash_value, None)

        return node

    def get_node_by_id(self, node_id: str) -> HashRingNode | None:
        """Get a node by its ID."""
        return self._nodes.get(node_id)

    def has_node(self, node_id: str) -> bool:
        """Check if a node exists in the ring."""
        return node_id in self._nodes

    def node_count(self) -> int:
        """Get the number of nodes in the ring."""
        return len(self._nodes)

    def get_all_nodes(self) -> list[HashRingNode]:
        """Get all nodes in the ring."""
        return list(self._nodes.values())

    # =========================================================================
    # Lookup Operations
    # =========================================================================

    def get_node(self, key: str) -> HashRingNode | None:
        """
        Get the node responsible for a key.

        Uses consistent hashing to find the first node on the ring
        at or after the key's hash position.

        Args:
            key: The key to look up (e.g., job_id).

        Returns:
            The responsible node, or None if ring is empty.
        """
        if not self._ring_positions:
            return None

        hash_value = self._hash(key)

        # Find the first position >= hash_value (clockwise lookup)
        index = bisect.bisect_left(self._ring_positions, hash_value)

        # Wrap around if we're past the end
        if index >= len(self._ring_positions):
            index = 0

        position = self._ring_positions[index]
        node_id = self._position_to_node[position]

        return self._nodes.get(node_id)

    def get_nodes(self, key: str, count: int = 1) -> list[HashRingNode]:
        """
        Get multiple nodes for a key (for replication/failover).

        Returns up to `count` distinct nodes, starting from the
        node responsible for the key and moving clockwise.

        Args:
            key: The key to look up (e.g., job_id).
            count: Number of nodes to return.

        Returns:
            List of nodes, may be fewer than count if not enough nodes.
        """
        if not self._ring_positions:
            return []

        # Limit count to number of actual nodes
        count = min(count, len(self._nodes))
        if count == 0:
            return []

        hash_value = self._hash(key)
        index = bisect.bisect_left(self._ring_positions, hash_value)

        result: list[HashRingNode] = []
        seen_node_ids: set[str] = set()

        # Walk around the ring collecting distinct nodes
        ring_size = len(self._ring_positions)
        for offset in range(ring_size):
            position_index = (index + offset) % ring_size
            position = self._ring_positions[position_index]
            node_id = self._position_to_node[position]

            if node_id not in seen_node_ids:
                node = self._nodes.get(node_id)
                if node:
                    result.append(node)
                    seen_node_ids.add(node_id)

                    if len(result) >= count:
                        break

        return result

    def get_owner_id(self, key: str) -> str | None:
        """
        Get the node ID responsible for a key.

        Convenience method that returns just the node_id.

        Args:
            key: The key to look up (e.g., job_id).

        Returns:
            The responsible node ID, or None if ring is empty.
        """
        node = self.get_node(key)
        return node.node_id if node else None

    def is_owner(self, key: str, node_id: str) -> bool:
        """
        Check if a specific node owns a key.

        Args:
            key: The key to check (e.g., job_id).
            node_id: The node ID to check ownership for.

        Returns:
            True if the node owns the key.
        """
        owner_id = self.get_owner_id(key)
        return owner_id == node_id

    # =========================================================================
    # Statistics
    # =========================================================================

    def get_distribution(self, sample_keys: list[str]) -> dict[str, int]:
        """
        Get the distribution of sample keys across nodes.

        Useful for testing/debugging ring balance.

        Args:
            sample_keys: List of keys to check.

        Returns:
            Dict mapping node_id -> count of keys.
        """
        distribution: dict[str, int] = {node_id: 0 for node_id in self._nodes}

        for key in sample_keys:
            owner_id = self.get_owner_id(key)
            if owner_id:
                distribution[owner_id] += 1

        return distribution

    def get_ring_info(self) -> dict:
        """Get information about the ring state."""
        return {
            "node_count": len(self._nodes),
            "virtual_node_count": len(self._ring_positions),
            "replicas_per_node": self._replicas,
            "nodes": {
                node_id: {
                    "tcp_host": node.tcp_host,
                    "tcp_port": node.tcp_port,
                    "weight": node.weight,
                }
                for node_id, node in self._nodes.items()
            },
        }

    # =========================================================================
    # Internal Methods
    # =========================================================================

    def _hash(self, key: str) -> int:
        """
        Hash a key to a position on the ring.

        Uses MD5 for consistent, well-distributed hashes.
        Returns an integer in the range [0, 2^32).
        """
        digest = hashlib.md5(key.encode("utf-8")).digest()
        # Use first 4 bytes as unsigned int
        return int.from_bytes(digest[:4], byteorder="big")

    def clear(self) -> None:
        """Remove all nodes from the ring."""
        self._ring_positions.clear()
        self._position_to_node.clear()
        self._nodes.clear()
