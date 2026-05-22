"""
Learner promotion / eviction coordinator (AD-52 §7).

Leader-side bookkeeping. Tracks per-learner commit-index progress and
yields PromoteDecision when the learner is close enough to the leader,
or EvictDecision when the learner has exceeded its max lifetime.

The coordinator does NOT propose the resulting Raft entries — it just
decides. The caller (BootstrapCoordinator on the bootstrap path,
JoinCoordinator on the join path, or a dedicated leader loop) proposes
Promote / Remove against the Raft log.

Outside the deterministic apply layer (AD-52 §15) so monotonic time is
fine here.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


_DEFAULT_PROMOTE_THRESHOLD: int = 256
_DEFAULT_MAX_LIFETIME_SECONDS: float = 30 * 60.0


@dataclass(slots=True)
class LearnerProgress:
    """
    Per-learner tracking record.

    Fields:
        node_id                     uuid4() of the learner.
        added_at_monotonic          time.monotonic() when AddLearner was
                                    committed on this leader.
        last_observed_commit_index  Last commit_index this leader has
                                    seen from the learner's
                                    AppendEntriesResponse.
        last_progress_at_monotonic  time.monotonic() of the last commit-
                                    index increase. Used by the lifetime
                                    timer.
    """

    node_id: str
    added_at_monotonic: float
    last_observed_commit_index: int = 0
    last_progress_at_monotonic: float = 0.0


@dataclass(frozen=True, slots=True)
class PromoteDecision:
    node_id: str
    leader_commit_index: int
    learner_commit_index: int


@dataclass(frozen=True, slots=True)
class EvictDecision:
    node_id: str
    reason: str
    elapsed_seconds: float


class LearnerCoordinator:
    """
    Per-cluster leader-side learner coordinator. One instance per
    JointConsensusStateMachine. Lifecycle:

      on_learner_added       — when AddLearner commits locally.
      on_learner_progress    — for each AppendEntriesResponse from a
                               learner. Pass the learner's reported
                               commit_index and the leader's.
      on_learner_promoted    — when Promote commits locally.
      on_learner_removed     — when Remove commits locally (any reason).
      tick                   — periodic; returns the list of pending
                               decisions for the caller to propose.
    """

    __slots__ = (
        "_promote_threshold",
        "_max_lifetime_seconds",
        "_learners",
    )

    def __init__(
        self,
        promote_threshold: int = _DEFAULT_PROMOTE_THRESHOLD,
        max_lifetime_seconds: float = _DEFAULT_MAX_LIFETIME_SECONDS,
    ) -> None:
        if promote_threshold < 1:
            raise ValueError("promote_threshold must be >= 1")
        if max_lifetime_seconds < 1.0:
            raise ValueError("max_lifetime_seconds must be >= 1.0")

        self._promote_threshold = promote_threshold
        self._max_lifetime_seconds = max_lifetime_seconds
        self._learners: dict[str, LearnerProgress] = {}

    def on_learner_added(self, node_id: str) -> None:
        now_monotonic = time.monotonic()
        self._learners[node_id] = LearnerProgress(
            node_id=node_id,
            added_at_monotonic=now_monotonic,
            last_progress_at_monotonic=now_monotonic,
        )

    def on_learner_progress(
        self,
        node_id: str,
        learner_commit_index: int,
    ) -> None:
        progress = self._learners.get(node_id)
        if progress is None:
            return
        if learner_commit_index > progress.last_observed_commit_index:
            progress.last_observed_commit_index = learner_commit_index
            progress.last_progress_at_monotonic = time.monotonic()

    def on_learner_promoted(self, node_id: str) -> None:
        self._learners.pop(node_id, None)

    def on_learner_removed(self, node_id: str) -> None:
        self._learners.pop(node_id, None)

    def tick(
        self,
        leader_commit_index: int,
    ) -> tuple[list[PromoteDecision], list[EvictDecision]]:
        """
        Sweep the learner table once. Returns two lists:
          - promotions ready (within threshold).
          - evictions ready (exceeded lifetime).
        Caller proposes the corresponding Promote / Remove entries.
        """
        if leader_commit_index < 0:
            raise ValueError("leader_commit_index must be >= 0")

        now_monotonic = time.monotonic()
        ready_promotions: list[PromoteDecision] = []
        ready_evictions: list[EvictDecision] = []

        # Sort for AD-52 §15 determinism — even though this isn't on the
        # apply layer, deterministic ordering simplifies replay debugging.
        for node_id in sorted(self._learners.keys()):
            progress = self._learners[node_id]
            commit_gap = leader_commit_index - progress.last_observed_commit_index
            elapsed_since_add = now_monotonic - progress.added_at_monotonic

            if commit_gap <= self._promote_threshold:
                ready_promotions.append(
                    PromoteDecision(
                        node_id=node_id,
                        leader_commit_index=leader_commit_index,
                        learner_commit_index=progress.last_observed_commit_index,
                    )
                )
                continue

            if elapsed_since_add >= self._max_lifetime_seconds:
                ready_evictions.append(
                    EvictDecision(
                        node_id=node_id,
                        reason="learner_max_lifetime_exceeded",
                        elapsed_seconds=elapsed_since_add,
                    )
                )

        return ready_promotions, ready_evictions

    def known_learners(self) -> frozenset[str]:
        return frozenset(self._learners.keys())

    def progress_of(self, node_id: str) -> LearnerProgress | None:
        return self._learners.get(node_id)
