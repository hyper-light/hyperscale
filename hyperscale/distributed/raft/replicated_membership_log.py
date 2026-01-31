"""
Replicated membership log for Raft-backed cluster membership.

Routes SWIM join/leave/suspect events through Raft consensus so
all nodes in the cluster process membership changes in the same
committed order. This prevents split-brain disagreements about
which nodes are alive.

Pattern:
    1. SWIM detects join/leave/suspect
    2. Event is proposed through Raft via the consensus coordinator
    3. State machine applies the event in committed order
    4. All replicas see the same membership sequence
"""

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .logging_models import RaftDebug, RaftInfo, RaftWarning

if TYPE_CHECKING:
    from hyperscale.logging import Logger

    from .gate_raft_job_manager import GateRaftJobManager
    from .raft_job_manager import RaftJobManager


@dataclass(slots=True)
class MembershipEvent:
    """
    A single cluster membership event.

    Attributes:
        event_type: One of "join", "leave", "suspect".
        node_id: The node that joined, left, or is suspected.
        node_addr: TCP address of the node (None for leave events).
        timestamp: Monotonic time when the event was detected locally.
    """

    event_type: str
    node_id: str
    node_addr: tuple[str, int] | None
    timestamp: float


class ReplicatedMembershipLog:
    """
    Raft-replicated membership event log.

    Buffers SWIM membership events locally and proposes them
    through Raft for total-order replication. Maintains a
    bounded event history for each job.

    All nodes in the Raft group apply membership events in the
    same order, ensuring consistent views of cluster state.
    """

    __slots__ = (
        "_logger",
        "_node_id",
        "_pending_events",
        "_committed_events",
        "_max_history_per_job",
    )

    def __init__(
        self,
        logger: "Logger",
        node_id: str,
        max_history_per_job: int = 1_000,
    ) -> None:
        self._logger = logger
        self._node_id = node_id
        self._max_history_per_job = max_history_per_job

        # job_id -> list of pending (not yet proposed) events
        self._pending_events: dict[str, list[MembershipEvent]] = {}
        # job_id -> list of committed events (applied via state machine)
        self._committed_events: dict[str, list[MembershipEvent]] = {}

    def record_event(
        self,
        job_id: str,
        event_type: str,
        node_id: str,
        node_addr: tuple[str, int] | None = None,
    ) -> None:
        """
        Record a SWIM membership event for later replication.

        Synchronous hot path -- no Raft proposal, no I/O.
        Events are buffered until propose_pending() is called.
        """
        event = MembershipEvent(
            event_type=event_type,
            node_id=node_id,
            node_addr=node_addr,
            timestamp=time.monotonic(),
        )

        pending = self._pending_events.get(job_id)
        if pending is None:
            pending = []
            self._pending_events[job_id] = pending
        pending.append(event)

    def pending_count(self, job_id: str) -> int:
        """Number of pending events for a job."""
        pending = self._pending_events.get(job_id)
        return len(pending) if pending is not None else 0

    @property
    def total_pending(self) -> int:
        """Total pending events across all jobs."""
        return sum(len(events) for events in self._pending_events.values())

    async def propose_pending_manager(
        self,
        job_id: str,
        raft_job_manager: "RaftJobManager",
    ) -> int:
        """
        Propose all pending events for a job through manager Raft.

        Returns the number of successfully proposed events.
        """
        pending = self._pending_events.get(job_id)
        if not pending:
            return 0

        proposed = 0
        for event in list(pending):
            success = await raft_job_manager.node_membership_event(
                job_id=job_id,
                event_type=event.event_type,
                node_id=event.node_id,
                node_addr=event.node_addr,
            )
            if success:
                pending.remove(event)
                proposed += 1
            else:
                await self._logger.log(RaftWarning(
                    message=f"Membership event proposal rejected: {event.event_type} for {event.node_id}",
                    node_id=self._node_id,
                    job_id=job_id,
                ))
                break  # Stop proposing if rejected (likely not leader)

        return proposed

    async def propose_pending_gate(
        self,
        job_id: str,
        gate_raft_job_manager: "GateRaftJobManager",
    ) -> int:
        """
        Propose all pending events for a job through gate Raft.

        Returns the number of successfully proposed events.
        """
        pending = self._pending_events.get(job_id)
        if not pending:
            return 0

        proposed = 0
        for event in list(pending):
            success = await gate_raft_job_manager.gate_membership_event(
                job_id=job_id,
                event_type=event.event_type,
                node_id=event.node_id,
                node_addr=event.node_addr,
            )
            if success:
                pending.remove(event)
                proposed += 1
            else:
                await self._logger.log(RaftWarning(
                    message=f"Gate membership event proposal rejected: {event.event_type} for {event.node_id}",
                    node_id=self._node_id,
                    job_id=job_id,
                ))
                break

        return proposed

    def on_committed(
        self,
        job_id: str,
        event_type: str,
        node_id: str,
        node_addr: tuple[str, int] | None = None,
    ) -> None:
        """
        Record a committed membership event (applied by state machine).

        Called from the Raft state machine when a membership event
        is committed. Maintains bounded history per job.
        """
        committed = self._committed_events.get(job_id)
        if committed is None:
            committed = []
            self._committed_events[job_id] = committed

        committed.append(MembershipEvent(
            event_type=event_type,
            node_id=node_id,
            node_addr=node_addr,
            timestamp=time.monotonic(),
        ))

        # Bounded history: evict oldest
        if len(committed) > self._max_history_per_job:
            excess = len(committed) - self._max_history_per_job
            del committed[:excess]

    def get_committed_events(self, job_id: str) -> list[MembershipEvent]:
        """Get committed event history for a job."""
        return list(self._committed_events.get(job_id, []))

    def get_active_members(self, job_id: str) -> set[str]:
        """
        Derive active members from committed event history.

        Replays join/leave events to compute current membership.
        """
        members: set[str] = set()
        for event in self._committed_events.get(job_id, []):
            match event.event_type:
                case "join":
                    members.add(event.node_id)
                case "leave":
                    members.discard(event.node_id)
                case _:
                    pass  # "suspect" doesn't change membership
        return members

    def cleanup_job(self, job_id: str) -> None:
        """Remove all data for a job. Called on job completion."""
        self._pending_events.pop(job_id, None)
        self._committed_events.pop(job_id, None)

    def clear(self) -> None:
        """Release all data. Called on node shutdown."""
        self._pending_events.clear()
        self._committed_events.clear()
