"""
Watch-stream server (AD-52 §9).

The membership-distribution mechanism. Every cluster node opens a
long-lived stream; the server pushes deltas as they commit and snaps
to a fresh WatchSnapshot when a reconnecting client has fallen past
the ring buffer's retention.

This module owns:
  - The ring buffer of recent WatchDelta entries (default 16384).
  - Per-client subscription state (filter, last delivered LSN).
  - Snapshot construction at the current MembershipState.

It does NOT own the wire-level connection — that lives in the
server.protocol layer. WatchServer takes a per-stream WatchStreamSink
protocol and delegates the actual send.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .models.member_record import MemberRecord
from .models.watch_messages import (
    WatchDelta,
    WatchFilter,
    WatchOpen,
    WatchSnapshot,
)

if TYPE_CHECKING:
    from .membership_state import MembershipState


_DEFAULT_RING_BUFFER_SIZE: int = 16384


@runtime_checkable
class WatchStreamSink(Protocol):
    """A single per-client output channel. Implemented by the transport
    layer. The watch server calls send_snapshot() then a stream of
    send_delta() calls until the client disconnects."""

    async def send_snapshot(self, snapshot: WatchSnapshot) -> None:
        ...

    async def send_delta(self, delta: WatchDelta) -> None:
        ...

    @property
    def closed(self) -> bool:
        ...


@dataclass(slots=True)
class _ActiveWatcher:
    """Per-watcher bookkeeping on the server."""

    watcher_node_id: str
    sink: WatchStreamSink
    filter_spec: WatchFilter
    last_delivered_lsn: int = 0


class WatchServer:
    """
    Single-cluster server-side watch dispatcher. One instance per
    ClusterNode. The apply loop calls publish_delta() on every commit;
    the watch server fans out to active watchers respecting their
    filters.
    """

    __slots__ = (
        "_membership_state_provider",
        "_cluster_uuid_provider",
        "_ring_buffer_size",
        "_ring_buffer",
        "_active_watchers",
        "_watchers_lock",
    )

    def __init__(
        self,
        membership_state_provider,
        cluster_uuid_provider,
        ring_buffer_size: int = _DEFAULT_RING_BUFFER_SIZE,
    ) -> None:
        if ring_buffer_size < 1:
            raise ValueError("ring_buffer_size must be >= 1")
        self._membership_state_provider = membership_state_provider
        self._cluster_uuid_provider = cluster_uuid_provider
        self._ring_buffer_size = ring_buffer_size
        self._ring_buffer: deque[WatchDelta] = deque(maxlen=ring_buffer_size)
        self._active_watchers: dict[str, _ActiveWatcher] = {}
        self._watchers_lock = asyncio.Lock()

    async def open_watch(
        self,
        watch_open: WatchOpen,
        sink: WatchStreamSink,
    ) -> None:
        """
        Handle a fresh WatchOpen. Either resumes from
        watch_open.last_seen_lsn + 1 (if still in ring buffer) or sends
        a fresh WatchSnapshot. Then registers the sink for ongoing
        deltas.
        """
        active_watcher = _ActiveWatcher(
            watcher_node_id=watch_open.watcher_node_id,
            sink=sink,
            filter_spec=watch_open.watch_filter,
            last_delivered_lsn=watch_open.last_seen_lsn,
        )

        if self._can_resume_from_ring_buffer(watch_open.last_seen_lsn):
            # Replay missed deltas from the ring buffer.
            for delta in list(self._ring_buffer):
                if delta.lsn <= watch_open.last_seen_lsn:
                    continue
                if not self._delta_passes_filter(delta, active_watcher.filter_spec):
                    continue
                await sink.send_delta(delta)
                active_watcher.last_delivered_lsn = delta.lsn
        else:
            # Catastrophic disconnect or first connect — full snapshot.
            snapshot = self._build_snapshot()
            await sink.send_snapshot(snapshot)
            active_watcher.last_delivered_lsn = snapshot.snapshot_lsn

        async with self._watchers_lock:
            self._active_watchers[watch_open.watcher_node_id] = active_watcher

    async def close_watch(self, watcher_node_id: str) -> None:
        async with self._watchers_lock:
            self._active_watchers.pop(watcher_node_id, None)

    async def publish_delta(self, delta: WatchDelta) -> None:
        """Called by the apply loop on every committed entry. Adds to
        the ring buffer and fans out to all active watchers whose filter
        admits the delta."""
        self._ring_buffer.append(delta)
        async with self._watchers_lock:
            watchers = list(self._active_watchers.values())
        for watcher in watchers:
            if not self._delta_passes_filter(delta, watcher.filter_spec):
                continue
            if watcher.sink.closed:
                await self.close_watch(watcher.watcher_node_id)
                continue
            try:
                await watcher.sink.send_delta(delta)
                watcher.last_delivered_lsn = delta.lsn
            except Exception:
                # Sink error → drop the watcher. Per AD-23, slow / broken
                # consumers do not block other watchers.
                await self.close_watch(watcher.watcher_node_id)

    def _can_resume_from_ring_buffer(self, last_seen_lsn: int) -> bool:
        if last_seen_lsn == 0:
            # First connect; let the client decide via the snapshot path.
            return False
        if not self._ring_buffer:
            return False
        oldest_lsn = self._ring_buffer[0].lsn
        return last_seen_lsn >= oldest_lsn - 1

    def _delta_passes_filter(
        self,
        delta: WatchDelta,
        filter_spec: WatchFilter,
    ) -> bool:
        if filter_spec.entry_types and delta.entry_type not in filter_spec.entry_types:
            return False
        # node_id_subset filtering would require decoding the entry —
        # not done here (the client can drop based on the decoded entry).
        return True

    def _build_snapshot(self) -> WatchSnapshot:
        current_state = self._membership_state_provider()
        sorted_members = tuple(
            sorted(current_state.members.values(), key=lambda record: record.node_id)
        )
        return WatchSnapshot(
            snapshot_epoch=current_state.cluster_metadata.membership_epoch,
            snapshot_lsn=current_state.cluster_metadata.last_membership_lsn,
            membership_at_lsn=sorted_members,
            cluster_uuid=self._cluster_uuid_provider(),
            cluster_size=current_state.cluster_metadata.cluster_size,
        )
