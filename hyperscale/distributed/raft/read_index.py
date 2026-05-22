"""
ReadIndex — linearizable reads without writing the log (AD-52 §11).

Protocol per the Raft paper §6.4 / etcd:

  1. Reader (any voter) asks the leader for commit_index_at_read.
  2. Leader verifies it is still leader (quorum heartbeat exchange OR
     valid lease).
  3. Leader replies commit_index_at_read.
  4. Reader waits until its local apply_index >= commit_index_at_read.
  5. Reader serves the read from its local state machine.

This module owns step 2's quorum-verification handshake and step 4's
local apply-wait primitive. Step 1 + step 3 are the wire-protocol
messages; their type-level shape is provided here but the actual
serialization is the caller's responsibility (msgspec lives in AD-25).

Outside the deterministic apply layer (AD-52 §15) so monotonic time
and asyncio primitives are fine here.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass(frozen=True, slots=True)
class ReadIndexRequest:
    """Reader → Leader. The request_id is opaque; lets the leader batch
    multiple read requests into one quorum verification round-trip."""

    request_id: str
    reader_node_id: str


@dataclass(frozen=True, slots=True)
class ReadIndexResponse:
    """Leader → Reader. commit_index_at_read is the value the reader
    must wait for locally before serving the read."""

    request_id: str
    commit_index_at_read: int
    leader_term: int


class LeaderLeaseExpired(Exception):
    """Raised by the leader when its lease is no longer valid; the
    caller should fall back to the heartbeat-quorum path."""


class ReadIndexLeaderHandler:
    """
    Leader-side ReadIndex handler.

    Two operating modes:
      - heartbeat-quorum (always available): every ReadIndex triggers
        a heartbeat to all followers; commit_index_at_read returns once
        a quorum acks. Batching across concurrent requests amortizes
        the cost.
      - lease-based (opt-in, requires bounded clock skew): leader holds
        a lease for quorum_timeout/2. While the lease is valid, the
        leader answers ReadIndex without quorum verification. AD-52 §11
        states the lease is opt-in via --leader-lease-enabled because
        Hyperscale does not assume bounded clock skew in general.
    """

    __slots__ = (
        "_send_heartbeats_and_wait_quorum",
        "_lease_enabled",
        "_lease_duration_seconds",
        "_lease_acquired_at_monotonic",
        "_current_commit_index",
        "_current_term",
    )

    def __init__(
        self,
        send_heartbeats_and_wait_quorum: Callable[[], Awaitable[bool]],
        lease_enabled: bool = False,
        lease_duration_seconds: float = 2.5,
    ) -> None:
        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be > 0")
        self._send_heartbeats_and_wait_quorum = send_heartbeats_and_wait_quorum
        self._lease_enabled = lease_enabled
        self._lease_duration_seconds = lease_duration_seconds
        self._lease_acquired_at_monotonic: float = 0.0
        self._current_commit_index: int = 0
        self._current_term: int = 0

    def update_commit_index(self, commit_index: int) -> None:
        if commit_index > self._current_commit_index:
            self._current_commit_index = commit_index

    def update_term(self, term: int) -> None:
        if term > self._current_term:
            self._current_term = term

    def renew_lease(self) -> None:
        """Called by the replication loop on each successful heartbeat
        quorum. Resets the lease clock."""
        if self._lease_enabled:
            self._lease_acquired_at_monotonic = time.monotonic()

    async def handle(self, request: ReadIndexRequest) -> ReadIndexResponse:
        """
        Process a ReadIndex request. Returns the response payload.

        If the leader's lease is valid AND lease_enabled, returns
        immediately without quorum verification. Otherwise broadcasts
        a heartbeat round and waits for quorum ack.
        """
        if self._lease_enabled and self._is_lease_valid():
            return ReadIndexResponse(
                request_id=request.request_id,
                commit_index_at_read=self._current_commit_index,
                leader_term=self._current_term,
            )

        quorum_acked = await self._send_heartbeats_and_wait_quorum()
        if not quorum_acked:
            raise LeaderLeaseExpired(
                "leader could not verify quorum on ReadIndex — stepping down"
            )
        return ReadIndexResponse(
            request_id=request.request_id,
            commit_index_at_read=self._current_commit_index,
            leader_term=self._current_term,
        )

    def _is_lease_valid(self) -> bool:
        if self._lease_acquired_at_monotonic == 0.0:
            return False
        elapsed = time.monotonic() - self._lease_acquired_at_monotonic
        return elapsed < self._lease_duration_seconds


class ReadIndexReader:
    """
    Reader-side helper. wait_for_local_apply() blocks until the local
    apply_index reaches commit_index_at_read, then the caller serves
    the read from the local state machine.
    """

    __slots__ = ("_local_apply_index_provider", "_wakeup")

    def __init__(
        self,
        local_apply_index_provider: Callable[[], int],
    ) -> None:
        self._local_apply_index_provider = local_apply_index_provider
        self._wakeup = asyncio.Event()

    def notify_apply_advance(self) -> None:
        """Called by the apply loop on every commit-index advance."""
        self._wakeup.set()
        # Re-arm immediately; events are set-once + clear cycles.
        self._wakeup.clear()

    async def wait_for_local_apply(
        self,
        target_apply_index: int,
        timeout_seconds: float = 10.0,
    ) -> None:
        deadline_monotonic = time.monotonic() + timeout_seconds
        while True:
            if self._local_apply_index_provider() >= target_apply_index:
                return
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError(
                    f"local apply did not reach {target_apply_index} within "
                    f"{timeout_seconds}s"
                )
            try:
                await asyncio.wait_for(self._wakeup.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                continue
