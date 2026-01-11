"""
Hierarchical Failure Detector coordinating global and job-layer detection.

This is the main entry point for failure detection in a multi-job distributed system.
It coordinates:
- Global layer (TimingWheel): Is the machine/node alive?
- Job layer (JobSuspicionManager): Is the node participating in this specific job?

Key design decisions:
1. Global death implies job death - if a machine is dead, all jobs on it are affected
2. Job-specific suspicion is independent - a node can be slow for job A but fine for job B
3. Result routing uses job layer - for accuracy, check job-specific status
4. Reconciliation handles disagreements - global alive + job dead = escalate
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable

from .timing_wheel import TimingWheel, TimingWheelConfig
from .job_suspicion_manager import JobSuspicionManager, JobSuspicionConfig
from .suspicion_state import SuspicionState
from hyperscale.distributed.health.extension_tracker import (
    ExtensionTracker,
    ExtensionTrackerConfig,
)


# Type aliases
NodeAddress = tuple[str, int]
JobId = str


class NodeStatus(Enum):
    """Status of a node from the perspective of failure detection."""
    ALIVE = auto()          # Not suspected at any layer
    SUSPECTED_GLOBAL = auto()  # Suspected at global layer (machine may be down)
    SUSPECTED_JOB = auto()     # Suspected for specific job(s) only
    DEAD_GLOBAL = auto()       # Declared dead at global layer
    DEAD_JOB = auto()          # Declared dead for specific job


class FailureSource(Enum):
    """Source of a failure detection event."""
    GLOBAL = auto()   # From global timing wheel
    JOB = auto()      # From job-specific detection


@dataclass
class HierarchicalConfig:
    """Configuration for hierarchical failure detection."""
    # Global layer config
    global_min_timeout: float = 5.0
    global_max_timeout: float = 30.0

    # Job layer config
    job_min_timeout: float = 1.0
    job_max_timeout: float = 10.0

    # Timing wheel settings
    coarse_tick_ms: int = 1000
    fine_tick_ms: int = 100

    # Job polling settings
    poll_interval_far_ms: int = 1000
    poll_interval_near_ms: int = 50

    # Reconciliation settings
    reconciliation_interval_s: float = 5.0

    # Resource limits
    max_global_suspicions: int = 10000
    max_job_suspicions_per_job: int = 1000
    max_total_job_suspicions: int = 50000

    # AD-26: Adaptive healthcheck extension settings
    extension_base_deadline: float = 30.0
    extension_min_grant: float = 1.0
    extension_max_extensions: int = 5
    extension_warning_threshold: int = 1
    extension_grace_period: float = 10.0
    max_extension_trackers: int = 10000  # Hard cap to prevent memory exhaustion


@dataclass
class FailureEvent:
    """Event emitted when a node is declared dead."""
    node: NodeAddress
    source: FailureSource
    job_id: JobId | None  # Only set for JOB source
    incarnation: int
    timestamp: float = field(default_factory=time.monotonic)


class HierarchicalFailureDetector:
    """
    Coordinates hierarchical failure detection across global and job layers.

    Usage:
    1. Register suspicions at the appropriate layer:
       - Global: When SWIM probe times out (machine-level liveness)
       - Job: When job-specific communication times out

    2. Query status for routing decisions:
       - is_alive_global(node): Is the machine up?
       - is_alive_for_job(job_id, node): Is node responsive for this job?

    3. Handle failure events via callbacks:
       - on_global_death: Machine declared dead
       - on_job_death: Node dead for specific job

    Reconciliation:
    - If global layer marks node dead, all job suspicions are cleared (implied dead)
    - If job layer marks node dead but global shows alive, this is job-specific failure
    - Periodic reconciliation checks for inconsistencies
    """

    def __init__(
        self,
        config: HierarchicalConfig | None = None,
        on_global_death: Callable[[NodeAddress, int], None] | None = None,
        on_job_death: Callable[[JobId, NodeAddress, int], None] | None = None,
        get_n_members: Callable[[], int] | None = None,
        get_job_n_members: Callable[[JobId], int] | None = None,
        get_lhm_multiplier: Callable[[], float] | None = None,
    ) -> None:
        if config is None:
            config = HierarchicalConfig()

        self._config = config
        self._on_global_death = on_global_death
        self._on_job_death = on_job_death
        self._get_n_members = get_n_members
        self._get_job_n_members = get_job_n_members
        self._get_lhm_multiplier = get_lhm_multiplier

        # Initialize global layer (timing wheel)
        timing_wheel_config = TimingWheelConfig(
            coarse_tick_ms=config.coarse_tick_ms,
            fine_tick_ms=config.fine_tick_ms,
        )
        self._global_wheel = TimingWheel(
            config=timing_wheel_config,
            on_expired=self._handle_global_expiration,
        )

        # Initialize job layer (adaptive polling)
        job_config = JobSuspicionConfig(
            poll_interval_far_ms=config.poll_interval_far_ms,
            poll_interval_near_ms=config.poll_interval_near_ms,
            max_suspicions_per_job=config.max_job_suspicions_per_job,
            max_total_suspicions=config.max_total_job_suspicions,
        )
        self._job_manager = JobSuspicionManager(
            config=job_config,
            on_expired=self._handle_job_expiration,
            get_n_members=get_job_n_members,
            get_lhm_multiplier=get_lhm_multiplier,
        )

        # Track nodes declared dead at global level
        self._globally_dead: set[NodeAddress] = set()

        # Reconciliation task
        self._reconciliation_task: asyncio.Task | None = None
        self._running: bool = False

        # Lock for state coordination
        self._lock = asyncio.Lock()

        # Event history for debugging/monitoring
        self._recent_events: list[FailureEvent] = []
        self._max_event_history: int = 100

        # Stats
        self._global_deaths: int = 0
        self._job_deaths: int = 0
        self._reconciliations: int = 0
        self._job_suspicions_cleared_by_global: int = 0

        # AD-26: Per-node extension trackers for adaptive healthcheck extensions
        self._extension_trackers: dict[NodeAddress, ExtensionTracker] = {}
        self._extension_tracker_config = ExtensionTrackerConfig(
            base_deadline=config.extension_base_deadline,
            min_grant=config.extension_min_grant,
            max_extensions=config.extension_max_extensions,
            warning_threshold=config.extension_warning_threshold,
            grace_period=config.extension_grace_period,
        )

        # Extension stats
        self._extensions_requested: int = 0
        self._extensions_granted: int = 0
        self._extensions_denied: int = 0
        self._extension_warnings_sent: int = 0
        self._extension_trackers_cleaned: int = 0

    def _get_current_n_members(self) -> int:
        """Get current global member count."""
        if self._get_n_members:
            return self._get_n_members()
        return 1

    async def start(self) -> None:
        """Start the failure detector."""
        if self._running:
            return

        self._running = True
        self._global_wheel.start()
        self._reconciliation_task = asyncio.create_task(self._reconciliation_loop())

    async def stop(self) -> None:
        """Stop the failure detector."""
        self._running = False

        if self._reconciliation_task and not self._reconciliation_task.done():
            self._reconciliation_task.cancel()
            try:
                await self._reconciliation_task
            except asyncio.CancelledError:
                pass

        await self._global_wheel.stop()
        await self._job_manager.shutdown()

    # =========================================================================
    # Global Layer Operations
    # =========================================================================

    async def suspect_global(
        self,
        node: NodeAddress,
        incarnation: int,
        from_node: NodeAddress,
    ) -> bool:
        """
        Start or update a global (machine-level) suspicion.

        Call this when SWIM probes time out - indicates machine may be down.

        Returns True if suspicion was created/updated.
        """
        async with self._lock:
            # Don't suspect already-dead nodes
            if node in self._globally_dead:
                return False

            # Check if already suspected
            existing_state = await self._global_wheel.get_state(node)

            if existing_state:
                if incarnation < existing_state.incarnation:
                    return False  # Stale
                elif incarnation == existing_state.incarnation:
                    # Add confirmation
                    existing_state.add_confirmation(from_node)
                    # Update expiration based on new confirmation count
                    new_timeout = existing_state.calculate_timeout()
                    new_expiration = existing_state.start_time + new_timeout
                    await self._global_wheel.update_expiration(node, new_expiration)
                    return True
                else:
                    # Higher incarnation - remove old and create new
                    await self._global_wheel.remove(node)

            # Create new suspicion state
            lhm = self._get_lhm_multiplier() if self._get_lhm_multiplier else 1.0
            state = SuspicionState(
                node=node,
                incarnation=incarnation,
                start_time=time.monotonic(),
                min_timeout=self._config.global_min_timeout * lhm,
                max_timeout=self._config.global_max_timeout * lhm,
                n_members=self._get_current_n_members(),
            )
            state.add_confirmation(from_node)

            expiration = time.monotonic() + state.calculate_timeout()
            return await self._global_wheel.add(node, state, expiration)

    async def confirm_global(
        self,
        node: NodeAddress,
        incarnation: int,
        from_node: NodeAddress,
    ) -> bool:
        """
        Add confirmation to existing global suspicion.

        Returns True if confirmation was added.
        """
        async with self._lock:
            state = await self._global_wheel.get_state(node)
            if state and state.incarnation == incarnation:
                if state.add_confirmation(from_node):
                    # Update expiration
                    new_timeout = state.calculate_timeout()
                    new_expiration = state.start_time + new_timeout
                    await self._global_wheel.update_expiration(node, new_expiration)
                    return True
            return False

    async def refute_global(
        self,
        node: NodeAddress,
        incarnation: int,
    ) -> bool:
        """
        Refute global suspicion (node proved alive with higher incarnation).

        Returns True if suspicion was cleared.
        """
        async with self._lock:
            state = await self._global_wheel.get_state(node)
            if state and incarnation > state.incarnation:
                await self._global_wheel.remove(node)
                # Reset extension tracker - node is healthy again (AD-26)
                self.reset_extension_tracker(node)
                return True
            return False

    async def clear_global_death(self, node: NodeAddress) -> bool:
        """
        Clear a node's globally dead status (e.g., node rejoined).

        Returns True if node was marked as dead and is now cleared.
        """
        async with self._lock:
            if node in self._globally_dead:
                self._globally_dead.discard(node)
                return True
            return False

    # =========================================================================
    # AD-26: Adaptive Healthcheck Extensions
    # =========================================================================

    def _get_or_create_extension_tracker(self, node: NodeAddress) -> ExtensionTracker | None:
        """
        Get or create an ExtensionTracker for a node.

        Returns None if the maximum number of trackers has been reached.
        """
        if node not in self._extension_trackers:
            # Check resource limit to prevent memory exhaustion
            if len(self._extension_trackers) >= self._config.max_extension_trackers:
                return None
            worker_id = f"{node[0]}:{node[1]}"
            self._extension_trackers[node] = self._extension_tracker_config.create_tracker(
                worker_id
            )
        return self._extension_trackers[node]

    async def request_extension(
        self,
        node: NodeAddress,
        reason: str,
        current_progress: float,
    ) -> tuple[bool, float, str | None, bool]:
        """
        Request a deadline extension for a suspected node (AD-26).

        Workers can request extensions when busy with legitimate work.
        Extensions are granted with logarithmic decay: max(min_grant, base / 2^n).
        Progress must be demonstrated to get an extension.

        Args:
            node: The node requesting an extension.
            reason: Reason for requesting extension (for logging).
            current_progress: Current progress metric (must increase to show progress).

        Returns:
            Tuple of (granted, extension_seconds, denial_reason, is_warning).
            - granted: True if extension was granted
            - extension_seconds: Amount of time granted (0 if denied)
            - denial_reason: Reason for denial, or None if granted
            - is_warning: True if this is a warning about impending exhaustion
        """
        self._extensions_requested += 1

        async with self._lock:
            # Check if node is actually suspected at global level
            state = await self._global_wheel.get_state(node)
            if state is None:
                return (
                    False,
                    0.0,
                    "Node is not currently suspected",
                    False,
                )

            # Get or create tracker for this node
            tracker = self._get_or_create_extension_tracker(node)

            # Check if tracker creation was denied due to resource limit
            if tracker is None:
                self._extensions_denied += 1
                return (
                    False,
                    0.0,
                    f"Maximum extension trackers ({self._config.max_extension_trackers}) reached",
                    False,
                )

            # Request the extension
            granted, extension_seconds, denial_reason, is_warning = tracker.request_extension(
                reason=reason,
                current_progress=current_progress,
            )

            if granted:
                self._extensions_granted += 1

                # Extend the suspicion timer in the timing wheel
                current_expiration = state.start_time + state.calculate_timeout()
                new_expiration = tracker.get_new_deadline(
                    current_deadline=current_expiration,
                    grant=extension_seconds,
                )
                await self._global_wheel.update_expiration(node, new_expiration)

                if is_warning:
                    self._extension_warnings_sent += 1
            else:
                self._extensions_denied += 1

            return (granted, extension_seconds, denial_reason, is_warning)

    def reset_extension_tracker(self, node: NodeAddress) -> None:
        """
        Reset the extension tracker for a node.

        Call this when:
        - A node becomes healthy again (suspicion cleared)
        - A new workflow/job starts on the node
        """
        if node in self._extension_trackers:
            self._extension_trackers[node].reset()

    def remove_extension_tracker(self, node: NodeAddress) -> None:
        """
        Remove the extension tracker for a node.

        Call this when a node is declared dead to clean up resources.
        """
        self._extension_trackers.pop(node, None)

    def get_extension_tracker(self, node: NodeAddress) -> ExtensionTracker | None:
        """Get the extension tracker for a node (for debugging/monitoring)."""
        return self._extension_trackers.get(node)

    def get_extension_status(self, node: NodeAddress) -> dict[str, float | int | bool] | None:
        """
        Get extension status for a node.

        Returns None if no tracker exists for the node.
        """
        tracker = self._extension_trackers.get(node)
        if tracker is None:
            return None

        return {
            "extension_count": tracker.extension_count,
            "remaining_extensions": tracker.get_remaining_extensions(),
            "total_extended": tracker.total_extended,
            "is_exhausted": tracker.is_exhausted,
            "is_in_grace_period": tracker.is_in_grace_period,
            "grace_period_remaining": tracker.grace_period_remaining,
            "should_evict": tracker.should_evict,
            "warning_sent": tracker.warning_sent,
        }

    # =========================================================================
    # Job Layer Operations
    # =========================================================================

    async def suspect_job(
        self,
        job_id: JobId,
        node: NodeAddress,
        incarnation: int,
        from_node: NodeAddress,
    ) -> bool:
        """
        Start or update a job-specific suspicion.

        Call this when job-specific communication times out - node may be
        slow/unresponsive for this particular job.

        Returns True if suspicion was created/updated.
        """
        async with self._lock:
            # If globally dead, no need for job-specific suspicion
            if node in self._globally_dead:
                return False

        result = await self._job_manager.start_suspicion(
            job_id=job_id,
            node=node,
            incarnation=incarnation,
            from_node=from_node,
            min_timeout=self._config.job_min_timeout,
            max_timeout=self._config.job_max_timeout,
        )
        return result is not None

    async def confirm_job(
        self,
        job_id: JobId,
        node: NodeAddress,
        incarnation: int,
        from_node: NodeAddress,
    ) -> bool:
        """Add confirmation to job-specific suspicion."""
        return await self._job_manager.confirm_suspicion(
            job_id, node, incarnation, from_node
        )

    async def refute_job(
        self,
        job_id: JobId,
        node: NodeAddress,
        incarnation: int,
    ) -> bool:
        """Refute job-specific suspicion."""
        return await self._job_manager.refute_suspicion(job_id, node, incarnation)

    async def clear_job(self, job_id: JobId) -> int:
        """Clear all suspicions for a completed job."""
        return await self._job_manager.clear_job(job_id)

    async def suspect_node_for_job(
        self,
        job_id: JobId,
        node: NodeAddress,
        incarnation: int,
        min_timeout: float | None = None,
        max_timeout: float | None = None,
    ) -> bool:
        """
        Suspect a node at the job layer (AD-30).

        This is used when a node is globally alive but not responsive
        for a specific job (e.g., stuck workflows, no progress).

        Unlike suspect_job(), this method is called by the manager's
        responsiveness monitoring, not from gossip messages. The manager
        itself is the source of the suspicion.

        Args:
            job_id: The job for which the node is unresponsive.
            node: The node address (host, port).
            incarnation: The node's incarnation number.
            min_timeout: Optional minimum timeout override.
            max_timeout: Optional maximum timeout override.

        Returns:
            True if suspicion was created/updated, False otherwise.
        """
        async with self._lock:
            # Check global death first - if node is globally dead, no need
            # for job-layer suspicion
            if node in self._globally_dead:
                return False

        # Use node itself as the confirmer (self-suspicion from monitoring)
        result = await self._job_manager.start_suspicion(
            job_id=job_id,
            node=node,
            incarnation=incarnation,
            from_node=node,  # Self-referential for monitoring-driven suspicion
            min_timeout=min_timeout or self._config.job_min_timeout,
            max_timeout=max_timeout or self._config.job_max_timeout,
        )
        return result is not None

    # =========================================================================
    # Status Queries
    # =========================================================================

    async def is_alive_global(self, node: NodeAddress) -> bool:
        """
        Check if a node is alive at the global (machine) level.

        Returns False if:
        - Node is globally dead
        - Node is currently suspected at global level

        Use this for general routing decisions.
        """
        async with self._lock:
            if node in self._globally_dead:
                return False

        return not await self._global_wheel.contains(node)

    def is_alive_for_job(self, job_id: JobId, node: NodeAddress) -> bool:
        """
        Check if a node is alive for a specific job.

        Returns False if:
        - Node is globally dead
        - Node is suspected for this specific job

        Use this for job-specific routing (e.g., result delivery).
        """
        # Check global death first (sync check)
        if node in self._globally_dead:
            return False

        # Then check job-specific suspicion
        return not self._job_manager.is_suspected(job_id, node)

    async def get_node_status(self, node: NodeAddress) -> NodeStatus:
        """
        Get comprehensive status of a node.

        Returns the most severe status across all layers.
        """
        async with self._lock:
            if node in self._globally_dead:
                return NodeStatus.DEAD_GLOBAL

        if await self._global_wheel.contains(node):
            return NodeStatus.SUSPECTED_GLOBAL

        # Check if suspected for any job
        jobs = self._job_manager.get_jobs_suspecting(node)
        if jobs:
            return NodeStatus.SUSPECTED_JOB

        return NodeStatus.ALIVE

    def get_jobs_with_suspected_node(self, node: NodeAddress) -> list[JobId]:
        """Get all jobs where this node is suspected."""
        return self._job_manager.get_jobs_suspecting(node)

    def get_suspected_nodes_for_job(self, job_id: JobId) -> list[NodeAddress]:
        """Get all suspected nodes for a job."""
        return self._job_manager.get_suspected_nodes(job_id)

    # =========================================================================
    # Expiration Handlers
    # =========================================================================

    def _handle_global_expiration(
        self,
        node: NodeAddress,
        state: SuspicionState,
    ) -> None:
        """
        Handle global suspicion expiration - node declared dead.

        This is called synchronously by the timing wheel.
        """
        # Mark as globally dead
        self._globally_dead.add(node)
        self._global_deaths += 1

        # Clean up extension tracker for this node (AD-26)
        self.remove_extension_tracker(node)

        # Record event
        event = FailureEvent(
            node=node,
            source=FailureSource.GLOBAL,
            job_id=None,
            incarnation=state.incarnation,
        )
        self._record_event(event)

        # Clear all job suspicions for this node (implied dead)
        asyncio.create_task(self._clear_job_suspicions_for_node(node))

        # Call callback
        if self._on_global_death:
            try:
                self._on_global_death(node, state.incarnation)
            except Exception:
                pass

    def _handle_job_expiration(
        self,
        job_id: JobId,
        node: NodeAddress,
        incarnation: int,
    ) -> None:
        """
        Handle job suspicion expiration - node dead for this job.

        This is called synchronously by the job manager.
        """
        self._job_deaths += 1

        # Record event
        event = FailureEvent(
            node=node,
            source=FailureSource.JOB,
            job_id=job_id,
            incarnation=incarnation,
        )
        self._record_event(event)

        # Call callback
        if self._on_job_death:
            try:
                self._on_job_death(job_id, node, incarnation)
            except Exception:
                pass

    async def _clear_job_suspicions_for_node(self, node: NodeAddress) -> None:
        """Clear all job suspicions for a globally-dead node."""
        jobs = self._job_manager.get_jobs_suspecting(node)
        for job_id in jobs:
            # Refute with very high incarnation to ensure clearing
            await self._job_manager.refute_suspicion(job_id, node, 2**31)
            self._job_suspicions_cleared_by_global += 1

    def _record_event(self, event: FailureEvent) -> None:
        """Record a failure event for history/debugging."""
        self._recent_events.append(event)
        if len(self._recent_events) > self._max_event_history:
            self._recent_events.pop(0)

    # =========================================================================
    # Reconciliation
    # =========================================================================

    async def _reconciliation_loop(self) -> None:
        """
        Periodic reconciliation between global and job layers.

        Handles edge cases:
        - Job suspicions for globally-dead nodes (should be cleared)
        - Stale global death markers (node may have rejoined)
        """
        while self._running:
            try:
                await asyncio.sleep(self._config.reconciliation_interval_s)
                await self._reconcile()
            except asyncio.CancelledError:
                break
            except Exception:
                pass  # Don't let reconciliation errors stop the loop

    async def _reconcile(self) -> None:
        """Perform reconciliation between layers."""
        self._reconciliations += 1

        async with self._lock:
            # Clear job suspicions for globally-dead nodes
            for node in list(self._globally_dead):
                jobs = self._job_manager.get_jobs_suspecting(node)
                for job_id in jobs:
                    await self._job_manager.refute_suspicion(job_id, node, 2**31)
                    self._job_suspicions_cleared_by_global += 1

            # AD-26: Clean up extension trackers for nodes that are no longer suspected
            # and have been reset (idle). This prevents memory leaks from accumulating
            # trackers for nodes that have come and gone.
            stale_tracker_nodes: list[NodeAddress] = []
            for node, tracker in self._extension_trackers.items():
                # Only remove if:
                # 1. Node is not currently suspected (no active suspicion)
                # 2. Tracker has been reset (extension_count == 0)
                # 3. Node is not globally dead (those are cleaned up on death)
                is_suspected = await self._global_wheel.contains(node)
                if not is_suspected and tracker.extension_count == 0 and node not in self._globally_dead:
                    stale_tracker_nodes.append(node)

            for node in stale_tracker_nodes:
                self._extension_trackers.pop(node, None)
                self._extension_trackers_cleaned += 1

    # =========================================================================
    # LHM Integration
    # =========================================================================

    async def apply_lhm_adjustment(self, multiplier: float) -> dict[str, int]:
        """
        Apply LHM adjustment to both layers.

        When Local Health Multiplier changes (node under load), extend
        all timeouts proportionally to reduce false positives.

        Returns stats on adjustments made.
        """
        global_adjusted = await self._global_wheel.apply_lhm_adjustment(multiplier)

        # Job manager handles LHM via callback during polling

        return {
            "global_adjusted": global_adjusted,
        }

    # =========================================================================
    # Stats and Monitoring
    # =========================================================================

    def get_stats(self) -> dict[str, int | float]:
        """Get comprehensive statistics."""
        global_stats = self._global_wheel.get_stats()
        job_stats = self._job_manager.get_stats()

        return {
            # Global layer
            "global_suspected": global_stats["current_entries"],
            "global_deaths": self._global_deaths,
            "globally_dead_count": len(self._globally_dead),

            # Job layer
            "job_suspicions": job_stats["active_suspicions"],
            "job_deaths": self._job_deaths,
            "jobs_with_suspicions": job_stats["jobs_with_suspicions"],

            # Reconciliation
            "reconciliations": self._reconciliations,
            "job_suspicions_cleared_by_global": self._job_suspicions_cleared_by_global,

            # Timing wheel internals
            "wheel_entries_added": global_stats["entries_added"],
            "wheel_entries_expired": global_stats["entries_expired"],
            "wheel_cascade_count": global_stats["cascade_count"],

            # AD-26: Extension stats
            "extensions_requested": self._extensions_requested,
            "extensions_granted": self._extensions_granted,
            "extensions_denied": self._extensions_denied,
            "extension_warnings_sent": self._extension_warnings_sent,
            "active_extension_trackers": len(self._extension_trackers),
            "extension_trackers_cleaned": self._extension_trackers_cleaned,
        }

    def get_recent_events(self, limit: int = 10) -> list[FailureEvent]:
        """Get recent failure events for debugging."""
        return self._recent_events[-limit:]

    async def get_global_suspicion_state(
        self,
        node: NodeAddress,
    ) -> SuspicionState | None:
        """Get global suspicion state for a node (for debugging)."""
        return await self._global_wheel.get_state(node)

    def get_job_suspicion_state(
        self,
        job_id: JobId,
        node: NodeAddress,
    ):
        """Get job suspicion state (for debugging)."""
        return self._job_manager.get_suspicion(job_id, node)

    # =========================================================================
    # Synchronous Helpers (for SWIM protocol integration)
    # =========================================================================

    def is_suspected_global(self, node: NodeAddress) -> bool:
        """
        Synchronously check if node is suspected at global level.

        Note: This checks the timing wheel directly without async lock.
        Use for quick checks in SWIM protocol handlers.
        """
        if node in self._globally_dead:
            return True
        return self._global_wheel.contains_sync(node)

    def get_time_remaining_global(self, node: NodeAddress) -> float | None:
        """
        Get remaining timeout for global suspicion.

        Returns None if node is not suspected.
        """
        state = self._global_wheel.get_state_sync(node)
        if state:
            return state.time_remaining()
        return None

    def should_regossip_global(self, node: NodeAddress) -> bool:
        """
        Check if global suspicion should be re-gossiped.

        Returns False if node is not suspected.
        """
        state = self._global_wheel.get_state_sync(node)
        if state:
            return state.should_regossip()
        return False

    def mark_regossiped_global(self, node: NodeAddress) -> None:
        """Mark global suspicion as having been re-gossiped."""
        state = self._global_wheel.get_state_sync(node)
        if state:
            state.mark_regossiped()

    def get_stats_sync(self) -> dict[str, int | float]:
        """Synchronous version of get_stats."""
        return self.get_stats()

    # Debug attribute (set by HealthAwareServer)
    _node_port: int = 0
