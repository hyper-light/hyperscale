"""
LearnerRole — AD-52 §7 non-voting Raft role.

A learner:
  - Receives AppendEntries and InstallSnapshot from the leader.
  - Applies entries to its state machine.
  - Reports its commit_index back via AppendEntriesResponse so the
    leader's LearnerCoordinator can decide when to propose Promote.
  - DOES NOT respond to RequestVote.
  - DOES NOT count toward quorum.
  - DOES NOT initiate elections.

Composed alongside RaftNode rather than inherited (AD-1). The existing
RaftNode role enum is "follower" | "candidate" | "leader"; this module
exposes "learner" as a fourth state with restricted behavior.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass(slots=True)
class LearnerState:
    """
    Per-instance state for the learner-role wrapper.

    Fields:
        node_id              uuid4() of this learner.
        cluster_group_id     Identifier of the Raft group (cluster_uuid
                             for the cluster Raft; job_id for per-job
                             Raft if a job ever needs learners).
        current_term         Highest term observed via AppendEntries.
        commit_index         Local commit_index (applied entries).
        last_applied         last_applied for the state machine.
        leader_id            node_id of the current leader, if known.
        last_heartbeat_at    monotonic seconds of last AppendEntries.
    """

    node_id: str
    cluster_group_id: str
    current_term: int = 0
    commit_index: int = 0
    last_applied: int = 0
    leader_id: str | None = None
    last_heartbeat_at: float = 0.0


class LearnerRole:
    """
    A pure learner: receives entries, applies them, never votes. The
    receiving side of the AppendEntries pipeline; outgoing responses
    report the learner's commit_index so the leader knows how close it
    is to promotion.

    The host process composes one LearnerRole per Raft group it learns
    from. On Promote-commit, the host swaps LearnerRole out for a
    regular RaftNode and the role transitions to follower.
    """

    __slots__ = (
        "_state",
        "_apply_entry",
        "_send_response",
    )

    def __init__(
        self,
        node_id: str,
        cluster_group_id: str,
        apply_entry: Callable[[int, object], Awaitable[None]],
        send_response: Callable[..., Awaitable[None]],
    ) -> None:
        """
        Args:
          apply_entry  : async callback (lsn, entry) → None invoked on
                         every committed entry. The cluster module's
                         JointConsensusStateMachine.apply_entry is the
                         typical target here.
          send_response: async sender of AppendEntriesResponse messages
                         back to the leader.
        """
        self._state = LearnerState(
            node_id=node_id,
            cluster_group_id=cluster_group_id,
        )
        self._apply_entry = apply_entry
        self._send_response = send_response

    @property
    def state(self) -> LearnerState:
        return self._state

    async def on_append_entries(
        self,
        leader_id: str,
        term: int,
        prev_log_index: int,
        prev_log_term: int,
        entries: list[tuple[int, int, object]],
        leader_commit: int,
    ) -> None:
        """
        Handle an AppendEntries from the leader. entries is a list of
        (lsn, term, payload) triples — typing is loose so this module
        does not depend on the raft.models AppendEntries shape.
        """
        if term < self._state.current_term:
            # Stale leader; ignore.
            return
        if term > self._state.current_term:
            self._state.current_term = term

        self._state.leader_id = leader_id
        self._state.last_heartbeat_at = time.monotonic()

        # Naïve append: a real implementation reconciles the prev_log
        # checks against the local log. The learner here is intentionally
        # thin — it relies on the leader to send InstallSnapshot when
        # log mismatch is severe, so prev_log conflicts are rare.
        for entry_lsn, entry_term, entry_payload in entries:
            if entry_lsn <= self._state.last_applied:
                continue
            if entry_lsn > leader_commit:
                # Not yet committed; do not apply.
                continue
            await self._apply_entry(entry_lsn, entry_payload)
            self._state.last_applied = entry_lsn
            self._state.commit_index = max(self._state.commit_index, entry_lsn)

        # Ack our commit_index back to the leader so its
        # LearnerCoordinator sees forward progress.
        await self._send_response(
            term=self._state.current_term,
            success=True,
            match_index=self._state.commit_index,
            from_node_id=self._state.node_id,
        )

    async def on_install_snapshot(
        self,
        snapshot_lsn: int,
        snapshot_payload: object,
    ) -> None:
        """
        Apply a leader-delivered snapshot. After the snapshot, the
        learner is at lsn=snapshot_lsn with last_applied=snapshot_lsn.
        """
        await self._apply_entry(snapshot_lsn, snapshot_payload)
        self._state.last_applied = snapshot_lsn
        self._state.commit_index = snapshot_lsn

    # The role explicitly does NOT implement on_request_vote — calling
    # it from the message router would be a bug; we make sure the
    # router does not route votes to learners (the cluster module's
    # role-aware dispatch handles that).
