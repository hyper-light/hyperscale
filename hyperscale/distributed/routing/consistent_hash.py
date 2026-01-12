from __future__ import annotations

import asyncio
import bisect
import hashlib
from typing import Iterator


class ConsistentHashRing:
    __slots__ = (
        "_ring",
        "_sorted_keys",
        "_nodes",
        "_vnodes",
        "_lock",
    )

    def __init__(self, virtual_nodes: int = 150) -> None:
        if virtual_nodes < 1:
            raise ValueError("virtual_nodes must be >= 1")

        self._ring: dict[int, str] = {}
        self._sorted_keys: list[int] = []
        self._nodes: set[str] = set()
        self._vnodes = virtual_nodes
        self._lock = asyncio.Lock()

    def _hash(self, key: str) -> int:
        digest = hashlib.md5(key.encode(), usedforsecurity=False).digest()
        return int.from_bytes(digest[:4], byteorder="big")

    async def add_node(self, node_id: str) -> None:
        async with self._lock:
            if node_id in self._nodes:
                return

            self._nodes.add(node_id)

            for i in range(self._vnodes):
                vnode_key = f"{node_id}:vnode:{i}"
                hash_pos = self._hash(vnode_key)
                self._ring[hash_pos] = node_id

            self._sorted_keys = sorted(self._ring.keys())

    async def remove_node(self, node_id: str) -> None:
        async with self._lock:
            if node_id not in self._nodes:
                return

            self._nodes.discard(node_id)

            for i in range(self._vnodes):
                vnode_key = f"{node_id}:vnode:{i}"
                hash_pos = self._hash(vnode_key)
                self._ring.pop(hash_pos, None)

            self._sorted_keys = sorted(self._ring.keys())

    async def get_node(self, key: str) -> str | None:
        async with self._lock:
            if not self._sorted_keys:
                return None

            hash_pos = self._hash(key)
            idx = bisect.bisect_left(self._sorted_keys, hash_pos)

            if idx >= len(self._sorted_keys):
                idx = 0

            return self._ring[self._sorted_keys[idx]]

    async def get_backup(self, key: str) -> str | None:
        async with self._lock:
            if len(self._nodes) < 2:
                return None

            primary = await self._get_node_unlocked(key)
            if primary is None:
                return None

            hash_pos = self._hash(key)
            idx = bisect.bisect_left(self._sorted_keys, hash_pos)

            if idx >= len(self._sorted_keys):
                idx = 0

            ring_size = len(self._sorted_keys)
            for offset in range(1, ring_size):
                check_idx = (idx + offset) % ring_size
                candidate = self._ring[self._sorted_keys[check_idx]]
                if candidate != primary:
                    return candidate

            return None

    async def _get_node_unlocked(self, key: str) -> str | None:
        if not self._sorted_keys:
            return None

        hash_pos = self._hash(key)
        idx = bisect.bisect_left(self._sorted_keys, hash_pos)

        if idx >= len(self._sorted_keys):
            idx = 0

        return self._ring[self._sorted_keys[idx]]

    async def get_nodes_for_key(self, key: str, count: int = 2) -> list[str]:
        async with self._lock:
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

    async def get_all_nodes(self) -> list[str]:
        async with self._lock:
            return list(self._nodes)

    async def node_count(self) -> int:
        async with self._lock:
            return len(self._nodes)

    async def contains(self, node_id: str) -> bool:
        async with self._lock:
            return node_id in self._nodes

    async def get_nodes_iter(self) -> list[str]:
        async with self._lock:
            return list(self._nodes)

    async def key_distribution(self, sample_keys: list[str]) -> dict[str, int]:
        async with self._lock:
            distribution: dict[str, int] = {node: 0 for node in self._nodes}

        for key in sample_keys:
            node = await self.get_node(key)
            if node:
                distribution[node] += 1

        return distribution
