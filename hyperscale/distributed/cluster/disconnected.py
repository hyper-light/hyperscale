"""
Disconnected-mode soft-state cache (AD-52 §10).

Every node maintains an in-memory copy of the last-known cluster view
populated by the watch stream. Data-plane consumers read from this
cache, never directly from Raft.

Every cache read returns (value, staleness_ms) so the consumer can
choose between:

  - Hard-fresh required          → bypass cache, take a ReadIndex hit.
  - Bounded staleness acceptable → cache read if staleness < bound.
  - Best-effort                   → always cache, never block.

When the watch stream is disconnected for longer than
disconnected_mode_threshold (default 30s), the cache is still served
with growing staleness; the data plane keeps making local decisions
per AD-52 §10's robustness contract.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, TypeVar

from .models.member_record import MemberRecord


T = TypeVar("T")


_DEFAULT_DISCONNECTED_MODE_THRESHOLD_SECONDS: float = 30.0


class Freshness(str, Enum):
    HARD_FRESH = "hard_fresh"
    BOUNDED_STALE = "bounded_stale"
    BEST_EFFORT = "best_effort"


@dataclass(frozen=True, slots=True)
class CacheRead(Generic[T]):
    """Result of a cache read. staleness_ms is computed at call time;
    the caller chooses what to do with it based on the requested
    Freshness."""

    value: T
    staleness_ms: float
    is_stale: bool


@dataclass(slots=True)
class _ObservedView:
    members_by_id: dict[str, MemberRecord]
    epoch: int
    cluster_uuid: str
    last_updated_monotonic: float


class SoftStateCache:
    """
    Single instance per ClusterNode. Updated by the watch_client's
    on_snapshot/on_delta callbacks; read by every data-plane component.

    Outside the deterministic apply layer — monotonic time is allowed.
    """

    __slots__ = (
        "_view",
        "_disconnected_threshold_seconds",
        "_disconnected_mode_active",
        "_last_watch_update_monotonic",
    )

    def __init__(
        self,
        disconnected_threshold_seconds: float = _DEFAULT_DISCONNECTED_MODE_THRESHOLD_SECONDS,
    ) -> None:
        self._view: _ObservedView | None = None
        self._disconnected_threshold_seconds = disconnected_threshold_seconds
        self._disconnected_mode_active: bool = False
        self._last_watch_update_monotonic: float = 0.0

    def install_snapshot(
        self,
        members: list[MemberRecord],
        epoch: int,
        cluster_uuid: str,
    ) -> None:
        now_monotonic = time.monotonic()
        self._view = _ObservedView(
            members_by_id={record.node_id: record for record in members},
            epoch=epoch,
            cluster_uuid=cluster_uuid,
            last_updated_monotonic=now_monotonic,
        )
        self._last_watch_update_monotonic = now_monotonic
        self._disconnected_mode_active = False

    def upsert_member(self, record: MemberRecord, new_epoch: int) -> None:
        if self._view is None:
            return
        now_monotonic = time.monotonic()
        self._view.members_by_id[record.node_id] = record
        self._view.epoch = max(self._view.epoch, new_epoch)
        self._view.last_updated_monotonic = now_monotonic
        self._last_watch_update_monotonic = now_monotonic
        self._disconnected_mode_active = False

    def remove_member(self, node_id: str, new_epoch: int) -> None:
        if self._view is None:
            return
        now_monotonic = time.monotonic()
        self._view.members_by_id.pop(node_id, None)
        self._view.epoch = max(self._view.epoch, new_epoch)
        self._view.last_updated_monotonic = now_monotonic
        self._last_watch_update_monotonic = now_monotonic
        self._disconnected_mode_active = False

    def evaluate_disconnected_mode(self) -> bool:
        """Refresh the disconnected-mode flag based on time since last
        watch update. Returns the current flag."""
        if self._last_watch_update_monotonic == 0.0:
            # Pre-snapshot — not yet "disconnected" because we never
            # connected.
            return False
        elapsed = time.monotonic() - self._last_watch_update_monotonic
        self._disconnected_mode_active = elapsed >= self._disconnected_threshold_seconds
        return self._disconnected_mode_active

    @property
    def disconnected_mode_active(self) -> bool:
        return self._disconnected_mode_active

    def read_member(self, node_id: str) -> CacheRead[MemberRecord | None]:
        if self._view is None:
            return CacheRead(value=None, staleness_ms=0.0, is_stale=True)
        record = self._view.members_by_id.get(node_id)
        staleness_ms = (time.monotonic() - self._view.last_updated_monotonic) * 1000.0
        return CacheRead(
            value=record,
            staleness_ms=staleness_ms,
            is_stale=self._disconnected_mode_active,
        )

    def read_membership(self) -> CacheRead[list[MemberRecord]]:
        if self._view is None:
            return CacheRead(value=[], staleness_ms=0.0, is_stale=True)
        # Sorted for deterministic consumers (AD-52 §15-adjacent —
        # data-plane consumers benefit from stable iteration order).
        members = sorted(
            self._view.members_by_id.values(),
            key=lambda record: record.node_id,
        )
        staleness_ms = (time.monotonic() - self._view.last_updated_monotonic) * 1000.0
        return CacheRead(
            value=members,
            staleness_ms=staleness_ms,
            is_stale=self._disconnected_mode_active,
        )

    def epoch(self) -> int:
        if self._view is None:
            return 0
        return self._view.epoch

    def cluster_uuid(self) -> str:
        if self._view is None:
            return ""
        return self._view.cluster_uuid
