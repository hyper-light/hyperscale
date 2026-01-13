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

import asyncio
import bisect
import hashlib
from dataclasses import dataclass


@dataclass(slots=True)
class HashRingNode:
    """A node in the consistent hash ring."""

    node_id: str
    tcp_host: str
    tcp_port: int
    weight: int = 1


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

        replica_count = self._replicas * node.weight
        for replica_index in range(replica_count):
            key = f"{node_id}:{replica_index}"
            hash_value = self._hash(key)

            try:
                self._ring_positions.remove(hash_value)
            except ValueError:
                pass

            self._position_to_node.pop(hash_value, None)

        return node

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

    async def get_nodes(self, key: str, count: int = 1) -> list[HashRingNode]:
        async with self._lock:
            if not self._ring_positions:
                return []

            count = min(count, len(self._nodes))
            if count == 0:
                return []

            hash_value = self._hash(key)
            index = bisect.bisect_left(self._ring_positions, hash_value)

            result: list[HashRingNode] = []
            seen_node_ids: set[str] = set()

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
            distribution: dict[str, int] = {node_id: 0 for node_id in self._nodes}

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
