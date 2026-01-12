from __future__ import annotations

import threading
from time import time

from .lsn import LSN


class HybridLamportClock:
    """
    High-performance LSN generator for globally distributed systems.

    Generates 128-bit Hybrid Lamport Timestamps that are:
    - Globally unique: node_id + sequence guarantees no collisions
    - Globally orderable: Lamport logical time provides total order
    - Coordination-free: No network calls required
    - High throughput: 16M LSNs/ms/node (24-bit sequence)
    - Crash safe: Recovers from last persisted LSN
    - Clock independent: Logical time is authoritative, wall clock is advisory
    - Never fails: Sequence overflow advances logical time instead of failing

    Thread-safe via lock.
    """

    def __init__(
        self,
        node_id: int,
        logical_time: int = 0,
        sequence: int = 0,
    ) -> None:
        if not 0 <= node_id <= LSN.MAX_NODE_ID:
            raise ValueError(f"node_id must be 0-{LSN.MAX_NODE_ID}, got {node_id}")

        self._node_id = node_id
        self._logical_time = logical_time
        self._sequence = sequence
        self._last_wall_ms: int = 0
        self._lock = threading.Lock()

    @classmethod
    def recover(
        cls,
        node_id: int,
        last_lsn: LSN | None,
    ) -> HybridLamportClock:
        """
        Recover clock state from last known LSN.

        Call on startup after reading last LSN from WAL to ensure
        monotonicity across restarts.
        """
        if last_lsn is None:
            return cls(node_id)

        return cls(
            node_id=node_id,
            logical_time=last_lsn.logical_time + 1,
            sequence=0,
        )

    def generate(self) -> LSN:
        """
        Generate next LSN. Never fails. Never blocks on network. O(1).
        """
        with self._lock:
            current_wall_ms = int(time() * 1000) & LSN.MAX_WALL_CLOCK

            if current_wall_ms == self._last_wall_ms:
                self._sequence += 1

                if self._sequence > LSN.MAX_SEQUENCE:
                    self._logical_time += 1
                    self._sequence = 0
            else:
                self._last_wall_ms = current_wall_ms
                self._sequence = 0

            self._logical_time += 1

            return LSN(
                logical_time=self._logical_time,
                node_id=self._node_id,
                sequence=self._sequence,
                wall_clock=current_wall_ms,
            )

    def receive(self, remote_lsn: LSN) -> None:
        """
        Update logical clock on receiving message from another node.

        Implements Lamport rule: local_time = max(local_time, remote_time) + 1

        Call when receiving replicated WAL entries from other nodes.
        """
        with self._lock:
            if remote_lsn.logical_time >= self._logical_time:
                self._logical_time = remote_lsn.logical_time + 1

    def witness(self, remote_lsn: LSN) -> None:
        """
        Observe a remote LSN without generating new LSN.

        Updates logical time to maintain ordering but doesn't increment.
        Use when observing but not producing.
        """
        with self._lock:
            if remote_lsn.logical_time > self._logical_time:
                self._logical_time = remote_lsn.logical_time

    @property
    def current_logical_time(self) -> int:
        return self._logical_time

    @property
    def node_id(self) -> int:
        return self._node_id
