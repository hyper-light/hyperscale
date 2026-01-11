"""
Job Leadership Tracker - Encapsulates per-job leadership state and operations.

This class provides a clean, modular implementation of job leadership tracking
that can be shared between Manager and Gate nodes. It implements the Serf-style
UDP piggybacking protocol for distributed leadership consistency.

Key concepts:
- Per-job leadership: Each job has one leader (manager or gate) responsible
  for coordination, independent of SWIM cluster leadership
- Fencing tokens: Monotonic tokens prevent stale leaders from reasserting
  leadership after failover/recovery
- UDP piggybacking: Leadership claims are embedded in SWIM heartbeats for
  O(log n) propagation across the cluster
- Per-DC manager tracking: Gates track which manager leads each job in each DC

This is NOT about SWIM cluster leadership - it's about which node is
responsible for coordinating a specific job.

Asyncio Safety:
- All mutating operations acquire the internal asyncio.Lock
- Read-only operations do NOT acquire the lock (safe due to GIL for simple reads)
- Callers should use async methods when mutating state
"""

import asyncio
from dataclasses import dataclass, field
from typing import Generic, TypeVar


# Type variable for the metadata associated with each job's leadership
# For managers: layer_version (int)
# For gates: target_dc_count (int)
T = TypeVar('T')


@dataclass(slots=True)
class JobLeadership:
    """
    Leadership information for a single job.

    Attributes:
        leader_id: Node ID of the current leader
        leader_addr: TCP address (host, port) of the leader
        fencing_token: Monotonic token for consistency (higher = newer epoch)
    """
    leader_id: str
    leader_addr: tuple[str, int]
    fencing_token: int


@dataclass(slots=True)
class DCManagerLeadership:
    """
    Leadership information for a manager within a datacenter for a specific job.

    Used by gates to track which manager leads each job in each DC.
    When a manager fails, another manager takes over and the gate must
    be notified to update routing.

    Attributes:
        manager_id: Node ID of the manager leading this job in this DC
        manager_addr: TCP address (host, port) of the manager
        fencing_token: Monotonic token for consistency (higher = newer epoch)
    """
    manager_id: str
    manager_addr: tuple[str, int]
    fencing_token: int


@dataclass(slots=True)
class JobLeadershipTracker(Generic[T]):
    """
    Tracks per-job leadership state with fencing token consistency.

    This class encapsulates:
    - Which node leads each job (gate-to-gate or manager-to-manager)
    - Leader TCP addresses for routing
    - Fencing tokens for consistency during failover
    - Optional metadata per job (layer_version for managers, dc_count for gates)
    - Per-DC manager tracking (for gates tracking which manager leads each job in each DC)

    Asyncio Safety:
    - All mutating operations acquire the internal asyncio.Lock
    - Read-only operations do NOT acquire the lock (safe due to GIL for simple reads)
    - Use async methods (assume_leadership_async, etc.) for concurrent access

    Usage:
        tracker = JobLeadershipTracker[int](
            node_id="gate-abc123",
            node_addr=("127.0.0.1", 8000),
        )

        # Assume leadership of a new job (async for concurrent safety)
        await tracker.assume_leadership_async("job-123", metadata=3)

        # Process leadership claim from peer heartbeat
        await tracker.process_leadership_claim_async(
            job_id="job-456",
            claimer_id="gate-xyz789",
            claimer_addr=("127.0.0.1", 8002),
            fencing_token=5,
        )

        # Get leadership info for piggybacking in heartbeat (read-only, no lock needed)
        claims = tracker.get_leadership_claims()

        # Per-DC manager tracking (for gates)
        await tracker.update_dc_manager_async("job-123", "dc-east", "mgr-001", ("host", 8080), 1)
    """

    # This node's identity
    node_id: str
    node_addr: tuple[str, int]

    # Job leadership state
    # job_id -> JobLeadership
    _leaderships: dict[str, JobLeadership] = field(default_factory=dict)

    # Optional metadata per job (e.g., layer_version, target_dc_count)
    # job_id -> metadata
    _metadata: dict[str, T] = field(default_factory=dict)

    # Per-DC manager tracking (for gates)
    # job_id -> {dc_id -> DCManagerLeadership}
    _dc_managers: dict[str, dict[str, DCManagerLeadership]] = field(default_factory=dict)

    # Asyncio lock for concurrent access (initialized in __post_init__)
    _lock: asyncio.Lock = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Initialize non-field attributes after dataclass init."""
        # Create lock as instance attribute (can't use default_factory with Lock)
        object.__setattr__(self, '_lock', asyncio.Lock())

    # =========================================================================
    # Async Methods (with lock for concurrent safety)
    # =========================================================================

    async def assume_leadership_async(
        self,
        job_id: str,
        metadata: T | None = None,
        initial_token: int = 1,
    ) -> int:
        """
        Assume leadership of a job (async version with lock).

        Args:
            job_id: The job to lead
            metadata: Optional metadata to associate (layer_version, dc_count, etc.)
            initial_token: Starting fencing token (default 1)

        Returns:
            The fencing token assigned
        """
        async with self._lock:
            return self.assume_leadership(job_id, metadata, initial_token)

    async def takeover_leadership_async(
        self,
        job_id: str,
        metadata: T | None = None,
    ) -> int:
        """
        Take over leadership of a job (async version with lock).

        Args:
            job_id: The job to take over
            metadata: Optional metadata to associate

        Returns:
            The new fencing token
        """
        async with self._lock:
            return self.takeover_leadership(job_id, metadata)

    async def release_leadership_async(self, job_id: str) -> None:
        """Release leadership of a job (async version with lock)."""
        async with self._lock:
            self.release_leadership(job_id)

    async def process_leadership_claim_async(
        self,
        job_id: str,
        claimer_id: str,
        claimer_addr: tuple[str, int],
        fencing_token: int,
        metadata: T | None = None,
    ) -> bool:
        """
        Process a leadership claim from a peer's heartbeat (async version with lock).

        Args:
            job_id: The job being claimed
            claimer_id: Node ID of the claimer
            claimer_addr: TCP address of the claimer
            fencing_token: Claimer's fencing token
            metadata: Optional metadata from the claim

        Returns:
            True if the claim was accepted, False if rejected
        """
        async with self._lock:
            return self.process_leadership_claim(
                job_id, claimer_id, claimer_addr, fencing_token, metadata
            )

    # =========================================================================
    # Per-DC Manager Tracking (for Gates) - Async Methods
    # =========================================================================

    async def update_dc_manager_async(
        self,
        job_id: str,
        dc_id: str,
        manager_id: str,
        manager_addr: tuple[str, int],
        fencing_token: int,
    ) -> bool:
        """
        Update the manager leading a job in a specific datacenter (async with lock).

        Uses fencing tokens for consistency - only accepts updates with
        higher fencing tokens than currently tracked.

        Args:
            job_id: The job ID
            dc_id: The datacenter ID
            manager_id: Node ID of the manager
            manager_addr: TCP address of the manager
            fencing_token: Manager's fencing token for this job

        Returns:
            True if update was accepted, False if rejected (stale token)
        """
        async with self._lock:
            return self._update_dc_manager(job_id, dc_id, manager_id, manager_addr, fencing_token)

    def _update_dc_manager(
        self,
        job_id: str,
        dc_id: str,
        manager_id: str,
        manager_addr: tuple[str, int],
        fencing_token: int,
    ) -> bool:
        """
        Internal: Update DC manager without lock (caller must hold lock).
        """
        if job_id not in self._dc_managers:
            self._dc_managers[job_id] = {}

        current = self._dc_managers[job_id].get(dc_id)

        # Accept if:
        # 1. We don't have info for this DC yet, OR
        # 2. The fencing token is higher (newer leadership epoch)
        if current is None or fencing_token > current.fencing_token:
            self._dc_managers[job_id][dc_id] = DCManagerLeadership(
                manager_id=manager_id,
                manager_addr=manager_addr,
                fencing_token=fencing_token,
            )
            return True

        return False

    async def set_dc_manager_async(
        self,
        job_id: str,
        dc_id: str,
        manager_addr: tuple[str, int],
    ) -> None:
        """
        Set DC manager address without fencing (for initial assignment).

        Use this when first assigning a manager to a job in a DC.
        For updates after failures, use update_dc_manager_async which
        respects fencing tokens.
        """
        async with self._lock:
            if job_id not in self._dc_managers:
                self._dc_managers[job_id] = {}

            # Initialize with fencing token 0 if not exists, preserve if exists
            current = self._dc_managers[job_id].get(dc_id)
            current_token = current.fencing_token if current else 0

            self._dc_managers[job_id][dc_id] = DCManagerLeadership(
                manager_id="",  # Unknown initially
                manager_addr=manager_addr,
                fencing_token=current_token,
            )

    def get_dc_manager(self, job_id: str, dc_id: str) -> tuple[str, int] | None:
        """
        Get the manager address for a job in a specific DC.

        Read-only, no lock needed (GIL protects simple dict reads).
        """
        dc_managers = self._dc_managers.get(job_id)
        if dc_managers:
            leadership = dc_managers.get(dc_id)
            if leadership:
                return leadership.manager_addr
        return None

    def get_dc_manager_fencing_token(self, job_id: str, dc_id: str) -> int:
        """Get the fencing token for a DC manager (0 if unknown)."""
        dc_managers = self._dc_managers.get(job_id)
        if dc_managers:
            leadership = dc_managers.get(dc_id)
            if leadership:
                return leadership.fencing_token
        return 0

    def get_all_dc_managers(self, job_id: str) -> dict[str, tuple[str, int]]:
        """
        Get all DC manager addresses for a job.

        Returns:
            dict mapping dc_id -> (manager_host, manager_port)
        """
        dc_managers = self._dc_managers.get(job_id, {})
        return {
            dc_id: leadership.manager_addr
            for dc_id, leadership in dc_managers.items()
        }

    async def release_dc_managers_async(self, job_id: str) -> None:
        """Release all DC manager tracking for a job (async with lock)."""
        async with self._lock:
            self._dc_managers.pop(job_id, None)

    def get_dc_managers_snapshot(self) -> dict[str, dict[str, tuple[str, int]]]:
        """
        Get snapshot of all DC managers for all jobs.

        Used for state sync and piggybacking in heartbeats.

        Returns:
            dict mapping job_id -> {dc_id -> (manager_host, manager_port)}
        """
        return {
            job_id: {
                dc_id: leadership.manager_addr
                for dc_id, leadership in dc_managers.items()
            }
            for job_id, dc_managers in self._dc_managers.items()
        }

    # =========================================================================
    # Synchronous Methods (for backwards compatibility / non-concurrent use)
    # =========================================================================

    def assume_leadership(
        self,
        job_id: str,
        metadata: T | None = None,
        initial_token: int = 1,
    ) -> int:
        """
        Assume leadership of a job (typically on first submission).

        Args:
            job_id: The job to lead
            metadata: Optional metadata to associate (layer_version, dc_count, etc.)
            initial_token: Starting fencing token (default 1)

        Returns:
            The fencing token assigned
        """
        self._leaderships[job_id] = JobLeadership(
            leader_id=self.node_id,
            leader_addr=self.node_addr,
            fencing_token=initial_token,
        )
        if metadata is not None:
            self._metadata[job_id] = metadata
        return initial_token

    def takeover_leadership(
        self,
        job_id: str,
        metadata: T | None = None,
    ) -> int:
        """
        Take over leadership of a job (e.g., after peer failure).

        Increments the fencing token to establish a new leadership epoch.

        Args:
            job_id: The job to take over
            metadata: Optional metadata to associate

        Returns:
            The new fencing token
        """
        current = self._leaderships.get(job_id)
        old_token = current.fencing_token if current else 0
        new_token = old_token + 1

        self._leaderships[job_id] = JobLeadership(
            leader_id=self.node_id,
            leader_addr=self.node_addr,
            fencing_token=new_token,
        )
        if metadata is not None:
            self._metadata[job_id] = metadata

        return new_token

    def release_leadership(self, job_id: str) -> None:
        """
        Release leadership of a job (cleanup on completion).

        Args:
            job_id: The job to release
        """
        self._leaderships.pop(job_id, None)
        self._metadata.pop(job_id, None)

    def process_leadership_claim(
        self,
        job_id: str,
        claimer_id: str,
        claimer_addr: tuple[str, int],
        fencing_token: int,
        metadata: T | None = None,
    ) -> bool:
        """
        Process a leadership claim from a peer's heartbeat.

        Uses fencing tokens for consistency:
        - Accept if we don't know this job yet
        - Accept if the fencing token is higher (newer leadership epoch)
        - Reject if we have equal or higher token

        Args:
            job_id: The job being claimed
            claimer_id: Node ID of the claimer
            claimer_addr: TCP address of the claimer
            fencing_token: Claimer's fencing token
            metadata: Optional metadata from the claim

        Returns:
            True if the claim was accepted, False if rejected
        """
        current = self._leaderships.get(job_id)

        # Accept if:
        # 1. We don't know about this job yet, OR
        # 2. The fencing token is higher (newer leadership epoch)
        if current is None or fencing_token > current.fencing_token:
            self._leaderships[job_id] = JobLeadership(
                leader_id=claimer_id,
                leader_addr=claimer_addr,
                fencing_token=fencing_token,
            )
            if metadata is not None:
                self._metadata[job_id] = metadata
            return True

        return False

    def is_leader(self, job_id: str) -> bool:
        """Check if this node is the leader for the given job."""
        leadership = self._leaderships.get(job_id)
        return leadership is not None and leadership.leader_id == self.node_id

    def get_leader(self, job_id: str) -> str | None:
        """Get the node_id of the job leader, or None if unknown."""
        leadership = self._leaderships.get(job_id)
        return leadership.leader_id if leadership else None

    def get_leader_addr(self, job_id: str) -> tuple[str, int] | None:
        """Get the TCP address of the job leader, or None if unknown."""
        leadership = self._leaderships.get(job_id)
        return leadership.leader_addr if leadership else None

    def get_fencing_token(self, job_id: str) -> int:
        """Get the fencing token for a job (0 if unknown)."""
        leadership = self._leaderships.get(job_id)
        return leadership.fencing_token if leadership else 0

    def get_metadata(self, job_id: str) -> T | None:
        """Get the metadata associated with a job."""
        return self._metadata.get(job_id)

    def set_metadata(self, job_id: str, metadata: T) -> None:
        """Set metadata for a job."""
        self._metadata[job_id] = metadata

    def get_leadership_claims(self) -> dict[str, tuple[int, T | None]]:
        """
        Get leadership claims for jobs this node leads.

        Used for piggybacking in SWIM heartbeats.

        Returns:
            dict mapping job_id -> (fencing_token, metadata) for jobs we lead
        """
        result: dict[str, tuple[int, T | None]] = {}
        for job_id, leadership in self._leaderships.items():
            if leadership.leader_id == self.node_id:
                metadata = self._metadata.get(job_id)
                result[job_id] = (leadership.fencing_token, metadata)
        return result

    def get_all_jobs(self) -> list[str]:
        """Get all job IDs we're tracking (led by us or others)."""
        return list(self._leaderships.keys())

    def get_jobs_led_by(self, node_id: str) -> list[str]:
        """Get all job IDs led by a specific node."""
        return [
            job_id
            for job_id, leadership in self._leaderships.items()
            if leadership.leader_id == node_id
        ]

    def get_jobs_led_by_addr(self, addr: tuple[str, int]) -> list[str]:
        """Get all job IDs led by a node at a specific address."""
        return [
            job_id
            for job_id, leadership in self._leaderships.items()
            if leadership.leader_addr == addr
        ]

    def to_snapshot(self) -> tuple[
        dict[str, str],  # job_leaders
        dict[str, tuple[str, int]],  # job_leader_addrs
        dict[str, int],  # job_fencing_tokens
    ]:
        """
        Export state for snapshot/sync.

        Returns:
            Tuple of (job_leaders, job_leader_addrs, job_fencing_tokens) dicts
        """
        job_leaders: dict[str, str] = {}
        job_leader_addrs: dict[str, tuple[str, int]] = {}
        job_fencing_tokens: dict[str, int] = {}

        for job_id, leadership in self._leaderships.items():
            job_leaders[job_id] = leadership.leader_id
            job_leader_addrs[job_id] = leadership.leader_addr
            job_fencing_tokens[job_id] = leadership.fencing_token

        return job_leaders, job_leader_addrs, job_fencing_tokens

    def merge_from_snapshot(
        self,
        job_leaders: dict[str, str],
        job_leader_addrs: dict[str, tuple[str, int]],
        job_fencing_tokens: dict[str, int],
    ) -> None:
        """
        Merge state from a snapshot (e.g., from state sync).

        Only accepts entries with higher fencing tokens than current.

        Args:
            job_leaders: job_id -> leader_node_id
            job_leader_addrs: job_id -> (host, port)
            job_fencing_tokens: job_id -> fencing_token
        """
        for job_id, leader_id in job_leaders.items():
            fencing_token = job_fencing_tokens.get(job_id, 0)
            leader_addr = job_leader_addrs.get(job_id, ("", 0))

            self.process_leadership_claim(
                job_id=job_id,
                claimer_id=leader_id,
                claimer_addr=leader_addr,
                fencing_token=fencing_token,
            )

    def __len__(self) -> int:
        """Return the number of jobs being tracked."""
        return len(self._leaderships)

    def __contains__(self, job_id: str) -> bool:
        """Check if a job is being tracked."""
        return job_id in self._leaderships
