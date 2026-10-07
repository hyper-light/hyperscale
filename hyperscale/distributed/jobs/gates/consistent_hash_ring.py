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

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
import bisect
import hashlib
from dataclasses import dataclass
from itertools import filterfalse

from .hash_ring_node import HashRingNode


class ConsistentHashRing:
    """
    Async consistent hash ring for job-to-gate mapping.

    Uses MD5 hashing with virtual nodes (replicas) to achieve
    uniform distribution of jobs across gates. All mutating operations
    are protected by an async lock for thread safety.
    """

    __slots__ = (
        "_replicas",
        "_ring_positions",
        "_position_to_node",
        "_nodes",
        "_lock",
    )

    def __init__(self, replicas: int = 150):
        if replicas < 1:
            raise ValueError("replicas must be >= 1")

        self._replicas = replicas
        self._ring_positions: list[int] = []
        self._position_to_node: dict[int, str] = {}
        self._nodes: dict[str, HashRingNode] = {}
        self._lock = asyncio.Lock()

    async def add_node(
        self,
        node_id: str,
        tcp_host: str,
        tcp_port: int,
        weight: int = 1,
    ) -> None:
        async with self._lock:
            if node_id in self._nodes:
                self._remove_node_unlocked(node_id)

            node = HashRingNode(
                node_id=node_id,
                tcp_host=tcp_host,
                tcp_port=tcp_port,
                weight=weight,
            )
            self._nodes[node_id] = node

            replica_count = self._replicas * weight
            for replica_index in range(replica_count):
                key = f"{node_id}:{replica_index}"
                hash_value = self._hash(key)
                bisect.insort(self._ring_positions, hash_value)
                self._position_to_node[hash_value] = node_id

    async def remove_node(self, node_id: str) -> HashRingNode | None:
        async with self._lock:
            return self._remove_node_unlocked(node_id)

    def _remove_node_unlocked(self, node_id: str) -> HashRingNode | None:
        node = self._nodes.pop(node_id, None)
        if not node:
            return None

        positions_to_remove = self._unmap_replica_positions(node_id, node.weight)

        self._ring_positions = list(
            filterfalse(positions_to_remove.__contains__, self._ring_positions)
        )

        return node

    def _unmap_replica_positions(self, node_id: str, weight: int) -> set[int]:
        """Unmap every virtual-node position of ``node_id`` and return those positions."""
        positions_to_remove: set[int] = set()
        replica_count = self._replicas * weight
        for replica_index in range(replica_count):
            key = f"{node_id}:{replica_index}"
            hash_value = self._hash(key)
            positions_to_remove.add(hash_value)
            self._position_to_node.pop(hash_value, None)
        return positions_to_remove

    async def get_node(self, key: str) -> HashRingNode | None:
        async with self._lock:
            return self._get_node_unlocked(key)

    def _get_node_unlocked(self, key: str) -> HashRingNode | None:
        if not self._ring_positions:
            return None

        hash_value = self._hash(key)
        index = bisect.bisect_left(self._ring_positions, hash_value)

        if index >= len(self._ring_positions):
            index = 0

        position = self._ring_positions[index]
        node_id = self._position_to_node[position]

        return self._nodes.get(node_id)

    async def get_backup(self, key: str) -> HashRingNode | None:
        async with self._lock:
            if len(self._nodes) < 2:
                return None

            primary = self._get_node_unlocked(key)
            if primary is None:
                return None

            index = self._wrapped_start_index(key)

            return self._next_distinct_node(index, primary.node_id)

    def _wrapped_start_index(self, key: str) -> int:
        """The ring index the key hashes to, wrapping past the last position to 0 (lock held)."""
        hash_value = self._hash(key)
        index = bisect.bisect_left(self._ring_positions, hash_value)

        if index >= len(self._ring_positions):
            index = 0
        return index

    def _next_distinct_node(self, index: int, primary_node_id: str) -> HashRingNode | None:
        """Walk clockwise from ``index`` to the first position owned by a node other than the primary."""
        ring_size = len(self._ring_positions)
        for offset in range(1, ring_size):
            check_index = (index + offset) % ring_size
            candidate_id = self._position_to_node[self._ring_positions[check_index]]
            if candidate_id != primary_node_id:
                return self._nodes.get(candidate_id)

        return None

    async def get_nodes(self, key: str, count: int = 1) -> list[HashRingNode]:
        async with self._lock:
            if not self._ring_positions:
                return []

            count = min(count, len(self._nodes))
            if count == 0:
                return []

            hash_value = self._hash(key)
            index = bisect.bisect_left(self._ring_positions, hash_value)

            return self._collect_distinct_nodes(index, count)

    def _collect_distinct_nodes(self, index: int, count: int) -> list[HashRingNode]:
        """Walk clockwise from ``index`` collecting up to ``count`` distinct nodes (lock held)."""
        result: list[HashRingNode] = []
        seen_node_ids: set[str] = set()

        ring_size = len(self._ring_positions)
        for offset in range(ring_size):
            position_index = (index + offset) % ring_size
            position = self._ring_positions[position_index]
            node_id = self._position_to_node[position]

            if self._collect_node(node_id, seen_node_ids, result, count):
                break

        return result

    def _collect_node(
        self,
        node_id: str,
        seen_node_ids: set[str],
        result: list[HashRingNode],
        count: int,
    ) -> bool:
        """Collect an unseen, present node; True once ``count`` nodes are collected."""
        if node_id in seen_node_ids:
            return False
        node = self._nodes.get(node_id)
        if not node:
            return False
        result.append(node)
        seen_node_ids.add(node_id)

        return len(result) >= count

    async def get_owner_id(self, key: str) -> str | None:
        node = await self.get_node(key)
        return node.node_id if node else None

    async def is_owner(self, key: str, node_id: str) -> bool:
        owner_id = await self.get_owner_id(key)
        return owner_id == node_id

    async def get_node_by_id(self, node_id: str) -> HashRingNode | None:
        async with self._lock:
            return self._nodes.get(node_id)

    async def get_node_addr(self, node: HashRingNode | None) -> tuple[str, int] | None:
        if node is None:
            return None
        return (node.tcp_host, node.tcp_port)

    async def has_node(self, node_id: str) -> bool:
        async with self._lock:
            return node_id in self._nodes

    async def node_count(self) -> int:
        async with self._lock:
            return len(self._nodes)

    async def get_all_nodes(self) -> list[HashRingNode]:
        async with self._lock:
            return list(self._nodes.values())

    async def get_distribution(self, sample_keys: list[str]) -> dict[str, int]:
        async with self._lock:
            distribution: dict[str, int] = dict.fromkeys(self._nodes, 0)

        for key in sample_keys:
            owner_id = await self.get_owner_id(key)
            if owner_id:
                distribution[owner_id] += 1

        return distribution

    async def get_ring_info(self) -> dict:
        async with self._lock:
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

    async def clear(self) -> None:
        async with self._lock:
            self._ring_positions.clear()
            self._position_to_node.clear()
            self._nodes.clear()

    def _hash(self, key: str) -> int:
        digest = hashlib.md5(key.encode("utf-8"), usedforsecurity=False).digest()
        return int.from_bytes(digest[:4], byteorder="big")

_REHOMED = (
    HashRingNode,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
