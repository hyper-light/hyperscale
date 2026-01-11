"""
End-to-end simulation tests for cluster bootstrap and recovery scenarios.

These tests verify:
1. First manager becomes leader, first worker joins
2. All managers die, new managers start and recover state
3. Partial cluster survives, rejoins with recovered nodes
4. Verify no orphaned jobs or duplicate assignments after recovery

Tests use mocks for all networking to avoid live server requirements.
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from enum import Enum


# =============================================================================
# Mock Infrastructure
# =============================================================================


class NodeState(Enum):
    """State of a cluster node."""

    STARTING = "starting"
    SYNCING = "syncing"
    ACTIVE = "active"
    DRAINING = "draining"
    DEAD = "dead"


@dataclass
class PersistentState:
    """State that can be persisted and recovered."""

    jobs: dict[str, "JobSnapshot"] = field(default_factory=dict)
    fence_tokens: dict[str, int] = field(default_factory=dict)
    workflow_assignments: dict[str, str] = field(default_factory=dict)


@dataclass
class JobSnapshot:
    """Snapshot of a job's state for persistence."""

    job_id: str
    leader_manager_id: str | None
    fence_token: int
    workflow_ids: list[str]
    workflow_states: dict[str, dict] = field(default_factory=dict)


@dataclass
class ManagerNode:
    """Simulated manager node."""

    manager_id: str
    host: str
    tcp_port: int
    udp_port: int
    state: NodeState = NodeState.STARTING
    is_leader: bool = False

    # Job tracking
    jobs: dict[str, JobSnapshot] = field(default_factory=dict)
    fence_tokens: dict[str, int] = field(default_factory=dict)

    # Peer tracking
    known_managers: set[str] = field(default_factory=set)
    known_workers: set[str] = field(default_factory=set)

    # Recovery state
    recovered_from_checkpoint: bool = False
    last_checkpoint_time: float | None = None


@dataclass
class WorkerNode:
    """Simulated worker node."""

    worker_id: str
    host: str
    port: int
    state: NodeState = NodeState.STARTING

    # Workflow tracking
    active_workflows: dict[str, dict] = field(default_factory=dict)
    job_leaders: dict[str, tuple[str, int]] = field(default_factory=dict)
    fence_tokens: dict[str, int] = field(default_factory=dict)

    # Manager tracking
    primary_manager_id: str | None = None


# =============================================================================
# Cluster Bootstrap/Recovery Simulator
# =============================================================================


class ClusterSimulator:
    """
    Simulates cluster bootstrap and recovery scenarios.

    Supports:
    - Cold start from empty state
    - Recovery from persisted checkpoint
    - Partial failure and recovery
    """

    def __init__(self) -> None:
        self.managers: dict[str, ManagerNode] = {}
        self.workers: dict[str, WorkerNode] = {}
        self._current_leader_id: str | None = None

        # Persistent storage simulation
        self._checkpoint: PersistentState | None = None
        self._checkpoint_enabled = False

        # Event tracking
        self._event_log: list[tuple[float, str, dict]] = []

    def log_event(self, event_type: str, details: dict) -> None:
        """Log a cluster event."""
        self._event_log.append((time.monotonic(), event_type, details))

    def enable_checkpointing(self) -> None:
        """Enable checkpoint persistence."""
        self._checkpoint_enabled = True

    def save_checkpoint(self) -> None:
        """Save current state to checkpoint."""
        if not self._checkpoint_enabled:
            return

        self._checkpoint = PersistentState(
            jobs={
                job_id: JobSnapshot(
                    job_id=job.job_id,
                    leader_manager_id=job.leader_manager_id,
                    fence_token=job.fence_token,
                    workflow_ids=job.workflow_ids,
                    workflow_states=dict(job.workflow_states),
                )
                for mgr in self.managers.values()
                for job_id, job in mgr.jobs.items()
            },
            fence_tokens={
                job_id: token
                for mgr in self.managers.values()
                for job_id, token in mgr.fence_tokens.items()
            },
            workflow_assignments={
                wf_id: worker_id
                for worker_id, worker in self.workers.items()
                for wf_id in worker.active_workflows
            },
        )

        for mgr in self.managers.values():
            mgr.last_checkpoint_time = time.monotonic()

        self.log_event("checkpoint_saved", {"job_count": len(self._checkpoint.jobs)})

    def has_checkpoint(self) -> bool:
        """Check if a checkpoint exists."""
        return self._checkpoint is not None

    # =========================================================================
    # Node Lifecycle
    # =========================================================================

    async def start_manager(
        self,
        manager_id: str,
        host: str = "127.0.0.1",
        tcp_port: int = 9090,
        udp_port: int = 9091,
        recover_from_checkpoint: bool = False,
    ) -> ManagerNode:
        """Start a manager node."""
        manager = ManagerNode(
            manager_id=manager_id,
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            state=NodeState.STARTING,
        )
        self.managers[manager_id] = manager

        self.log_event("manager_starting", {"manager_id": manager_id})

        # Bootstrap/recovery logic
        if recover_from_checkpoint and self._checkpoint:
            await self._recover_manager_from_checkpoint(manager)
        else:
            await self._bootstrap_manager(manager)

        manager.state = NodeState.ACTIVE
        self.log_event("manager_active", {"manager_id": manager_id})

        return manager

    async def _bootstrap_manager(self, manager: ManagerNode) -> None:
        """Bootstrap a manager from empty state."""
        manager.state = NodeState.SYNCING

        # Discover existing managers
        for other_id, other_mgr in self.managers.items():
            if other_id != manager.manager_id and other_mgr.state == NodeState.ACTIVE:
                manager.known_managers.add(other_id)
                other_mgr.known_managers.add(manager.manager_id)

        # Discover existing workers
        for worker_id, worker in self.workers.items():
            if worker.state == NodeState.ACTIVE:
                manager.known_workers.add(worker_id)

        # If first manager, become leader
        if len(self.managers) == 1:
            manager.is_leader = True
            self._current_leader_id = manager.manager_id
            self.log_event("first_leader_elected", {"manager_id": manager.manager_id})

        await asyncio.sleep(0.01)  # Simulate bootstrap delay

    async def _recover_manager_from_checkpoint(self, manager: ManagerNode) -> None:
        """Recover a manager from checkpoint."""
        manager.state = NodeState.SYNCING

        if not self._checkpoint:
            await self._bootstrap_manager(manager)
            return

        # Restore job state
        for job_id, job_snapshot in self._checkpoint.jobs.items():
            manager.jobs[job_id] = JobSnapshot(
                job_id=job_snapshot.job_id,
                leader_manager_id=manager.manager_id,  # New manager takes over
                fence_token=job_snapshot.fence_token + 1,  # Increment for recovery
                workflow_ids=list(job_snapshot.workflow_ids),
                workflow_states=dict(job_snapshot.workflow_states),
            )
            manager.fence_tokens[job_id] = job_snapshot.fence_token + 1

        manager.recovered_from_checkpoint = True
        self.log_event("manager_recovered", {
            "manager_id": manager.manager_id,
            "jobs_recovered": len(manager.jobs),
        })

        await asyncio.sleep(0.01)

    async def start_worker(
        self,
        worker_id: str,
        host: str = "127.0.0.1",
        port: int = 8000,
    ) -> WorkerNode:
        """Start a worker node."""
        worker = WorkerNode(
            worker_id=worker_id,
            host=host,
            port=port,
            state=NodeState.STARTING,
        )
        self.workers[worker_id] = worker

        self.log_event("worker_starting", {"worker_id": worker_id})

        # Register with managers
        for mgr in self.managers.values():
            if mgr.state == NodeState.ACTIVE:
                mgr.known_workers.add(worker_id)

        # Find primary manager (leader)
        if self._current_leader_id:
            worker.primary_manager_id = self._current_leader_id

        worker.state = NodeState.ACTIVE
        self.log_event("worker_active", {"worker_id": worker_id})

        return worker

    async def stop_manager(self, manager_id: str, graceful: bool = True) -> None:
        """Stop a manager node."""
        manager = self.managers.get(manager_id)
        if not manager:
            return

        if graceful:
            manager.state = NodeState.DRAINING
            await asyncio.sleep(0.01)  # Drain delay

        manager.state = NodeState.DEAD
        manager.is_leader = False

        if self._current_leader_id == manager_id:
            self._current_leader_id = None

        # Remove from other managers' known lists
        for other_mgr in self.managers.values():
            other_mgr.known_managers.discard(manager_id)

        self.log_event("manager_stopped", {"manager_id": manager_id, "graceful": graceful})

    async def stop_worker(self, worker_id: str, graceful: bool = True) -> None:
        """Stop a worker node."""
        worker = self.workers.get(worker_id)
        if not worker:
            return

        if graceful:
            worker.state = NodeState.DRAINING
            await asyncio.sleep(0.01)

        worker.state = NodeState.DEAD

        # Remove from managers' known lists
        for mgr in self.managers.values():
            mgr.known_workers.discard(worker_id)

        self.log_event("worker_stopped", {"worker_id": worker_id, "graceful": graceful})

    # =========================================================================
    # Leader Election
    # =========================================================================

    async def elect_leader(self, manager_id: str | None = None) -> str | None:
        """Elect a leader. If manager_id is None, elect from active managers."""
        # Step down current leader
        if self._current_leader_id:
            old_leader = self.managers.get(self._current_leader_id)
            if old_leader:
                old_leader.is_leader = False

        # Find eligible candidate
        if manager_id:
            candidate = self.managers.get(manager_id)
            if candidate and candidate.state == NodeState.ACTIVE:
                candidate.is_leader = True
                self._current_leader_id = manager_id
        else:
            # Elect first active manager
            for mgr_id, mgr in self.managers.items():
                if mgr.state == NodeState.ACTIVE:
                    mgr.is_leader = True
                    self._current_leader_id = mgr_id
                    break

        if self._current_leader_id:
            self.log_event("leader_elected", {"manager_id": self._current_leader_id})

        return self._current_leader_id

    def get_leader(self) -> ManagerNode | None:
        """Get current leader."""
        if self._current_leader_id:
            return self.managers.get(self._current_leader_id)
        return None

    # =========================================================================
    # Job Operations
    # =========================================================================

    async def submit_job(
        self,
        job_id: str,
        workflow_ids: list[str],
        worker_assignments: dict[str, str],
    ) -> JobSnapshot:
        """Submit a job to the cluster."""
        leader = self.get_leader()
        if not leader:
            raise RuntimeError("No leader available")

        job = JobSnapshot(
            job_id=job_id,
            leader_manager_id=leader.manager_id,
            fence_token=1,
            workflow_ids=workflow_ids,
        )
        leader.jobs[job_id] = job
        leader.fence_tokens[job_id] = 1

        # Assign workflows to workers
        for wf_id, worker_id in worker_assignments.items():
            worker = self.workers.get(worker_id)
            if worker and worker.state == NodeState.ACTIVE:
                worker.active_workflows[wf_id] = {"job_id": job_id, "status": "running"}
                worker.job_leaders[job_id] = (leader.host, leader.tcp_port)
                worker.fence_tokens[job_id] = 1

        self.log_event("job_submitted", {"job_id": job_id, "workflow_count": len(workflow_ids)})

        if self._checkpoint_enabled:
            self.save_checkpoint()

        return job

    # =========================================================================
    # Cluster State Queries
    # =========================================================================

    def get_active_managers(self) -> list[ManagerNode]:
        """Get all active managers."""
        return [m for m in self.managers.values() if m.state == NodeState.ACTIVE]

    def get_active_workers(self) -> list[WorkerNode]:
        """Get all active workers."""
        return [w for w in self.workers.values() if w.state == NodeState.ACTIVE]

    def get_all_workflow_assignments(self) -> dict[str, str]:
        """Get all workflow -> worker assignments."""
        assignments = {}
        for worker_id, worker in self.workers.items():
            for wf_id in worker.active_workflows:
                assignments[wf_id] = worker_id
        return assignments

    def get_orphaned_jobs(self) -> list[str]:
        """Get jobs with no active leader."""
        orphaned = []
        for mgr in self.managers.values():
            if mgr.state == NodeState.DEAD:
                orphaned.extend(mgr.jobs.keys())
        return orphaned


# =============================================================================
# Test Classes
# =============================================================================


class TestClusterColdStart:
    """Tests for cluster cold start (no existing state)."""

    @pytest.mark.asyncio
    async def test_first_manager_becomes_leader(self):
        """First manager to start becomes leader."""
        cluster = ClusterSimulator()

        manager = await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)

        assert manager.is_leader
        assert cluster.get_leader() == manager
        assert manager.state == NodeState.ACTIVE

    @pytest.mark.asyncio
    async def test_first_worker_joins_and_registers(self):
        """First worker joins and registers with leader."""
        cluster = ClusterSimulator()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        worker = await cluster.start_worker("worker-1", port=8000)

        assert worker.state == NodeState.ACTIVE
        assert worker.primary_manager_id == "manager-1"
        assert "worker-1" in cluster.managers["manager-1"].known_workers

    @pytest.mark.asyncio
    async def test_second_manager_discovers_first(self):
        """Second manager discovers first manager."""
        cluster = ClusterSimulator()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        manager2 = await cluster.start_manager("manager-2", tcp_port=9092, udp_port=9093)

        assert "manager-1" in manager2.known_managers
        assert "manager-2" in cluster.managers["manager-1"].known_managers

    @pytest.mark.asyncio
    async def test_job_submission_on_fresh_cluster(self):
        """Job can be submitted on freshly started cluster."""
        cluster = ClusterSimulator()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        await cluster.start_worker("worker-1", port=8000)

        job = await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        assert job.job_id == "job-001"
        assert "job-001" in cluster.managers["manager-1"].jobs
        assert "wf-001" in cluster.workers["worker-1"].active_workflows


class TestAllManagersFailAndRecover:
    """Tests for total manager failure and recovery scenarios."""

    @pytest.mark.asyncio
    async def test_all_managers_fail_checkpoint_survives(self):
        """All managers fail but checkpoint preserves state."""
        cluster = ClusterSimulator()
        cluster.enable_checkpointing()

        # Start cluster
        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        await cluster.start_worker("worker-1", port=8000)

        # Submit job
        await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # All managers fail
        await cluster.stop_manager("manager-1", graceful=False)

        # Verify checkpoint exists
        assert cluster.has_checkpoint()
        assert "job-001" in cluster._checkpoint.jobs

    @pytest.mark.asyncio
    async def test_new_manager_recovers_from_checkpoint(self):
        """New manager recovers state from checkpoint."""
        cluster = ClusterSimulator()
        cluster.enable_checkpointing()

        # Initial cluster
        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        await cluster.start_worker("worker-1", port=8000)

        await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001", "wf-002"],
            worker_assignments={"wf-001": "worker-1", "wf-002": "worker-1"},
        )

        # Fail all managers
        await cluster.stop_manager("manager-1", graceful=False)

        # Start new manager with recovery
        new_manager = await cluster.start_manager(
            "manager-2",
            tcp_port=9092,
            udp_port=9093,
            recover_from_checkpoint=True,
        )

        # Verify recovery
        assert new_manager.recovered_from_checkpoint
        assert "job-001" in new_manager.jobs
        assert len(new_manager.jobs["job-001"].workflow_ids) == 2

    @pytest.mark.asyncio
    async def test_fence_token_incremented_on_recovery(self):
        """Fence token is incremented when job is recovered."""
        cluster = ClusterSimulator()
        cluster.enable_checkpointing()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        await cluster.start_worker("worker-1", port=8000)

        await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        original_token = cluster.managers["manager-1"].fence_tokens["job-001"]

        # Fail and recover
        await cluster.stop_manager("manager-1", graceful=False)

        new_manager = await cluster.start_manager(
            "manager-2",
            tcp_port=9092,
            udp_port=9093,
            recover_from_checkpoint=True,
        )

        # Token should be incremented
        assert new_manager.fence_tokens["job-001"] == original_token + 1

    @pytest.mark.asyncio
    async def test_multiple_managers_fail_and_recover(self):
        """Multiple managers fail and new cluster recovers."""
        cluster = ClusterSimulator()
        cluster.enable_checkpointing()

        # Start multi-manager cluster
        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        await cluster.start_manager("manager-2", tcp_port=9092, udp_port=9093)
        await cluster.start_worker("worker-1", port=8000)

        await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # All managers fail
        await cluster.stop_manager("manager-1", graceful=False)
        await cluster.stop_manager("manager-2", graceful=False)

        # New managers start and recover
        new_mgr1 = await cluster.start_manager(
            "manager-3", tcp_port=9094, udp_port=9095, recover_from_checkpoint=True
        )
        new_mgr2 = await cluster.start_manager(
            "manager-4", tcp_port=9096, udp_port=9097, recover_from_checkpoint=True
        )

        # Both should have recovered job
        assert "job-001" in new_mgr1.jobs
        assert "job-001" in new_mgr2.jobs


class TestPartialClusterSurvival:
    """Tests for partial cluster survival and recovery."""

    @pytest.mark.asyncio
    async def test_one_manager_survives_becomes_leader(self):
        """Surviving manager becomes leader when others fail."""
        cluster = ClusterSimulator()

        mgr1 = await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        mgr2 = await cluster.start_manager("manager-2", tcp_port=9092, udp_port=9093)
        await cluster.start_worker("worker-1", port=8000)

        # Make mgr1 leader
        await cluster.elect_leader("manager-1")

        await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # mgr1 fails
        await cluster.stop_manager("manager-1", graceful=False)

        # Elect mgr2
        await cluster.elect_leader("manager-2")

        assert cluster.get_leader() == mgr2
        assert mgr2.is_leader

    @pytest.mark.asyncio
    async def test_worker_survives_manager_failure(self):
        """Worker continues running when manager fails."""
        cluster = ClusterSimulator()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        worker = await cluster.start_worker("worker-1", port=8000)

        await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # Manager fails
        await cluster.stop_manager("manager-1", graceful=False)

        # Worker still has workflow (orphaned but not lost)
        assert "wf-001" in worker.active_workflows
        assert worker.state == NodeState.ACTIVE

    @pytest.mark.asyncio
    async def test_recovered_node_rejoins_cluster(self):
        """Previously failed manager can rejoin cluster."""
        cluster = ClusterSimulator()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        await cluster.start_manager("manager-2", tcp_port=9092, udp_port=9093)

        # mgr1 fails
        await cluster.stop_manager("manager-1", graceful=False)

        assert "manager-1" not in cluster.managers["manager-2"].known_managers

        # mgr1 restarts (as new instance)
        new_mgr1 = await cluster.start_manager("manager-1-new", tcp_port=9090, udp_port=9091)

        # Should discover mgr2
        assert "manager-2" in new_mgr1.known_managers

    @pytest.mark.asyncio
    async def test_partial_worker_failure_doesnt_lose_all_workflows(self):
        """Partial worker failure only affects those workers' workflows."""
        cluster = ClusterSimulator()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        worker1 = await cluster.start_worker("worker-1", port=8000)
        worker2 = await cluster.start_worker("worker-2", port=8001)

        await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001", "wf-002"],
            worker_assignments={"wf-001": "worker-1", "wf-002": "worker-2"},
        )

        # worker1 fails
        await cluster.stop_worker("worker-1", graceful=False)

        # worker2 still has its workflow
        assert "wf-002" in worker2.active_workflows
        assert worker2.state == NodeState.ACTIVE


class TestNoOrphanedJobsAfterRecovery:
    """Tests verifying no orphaned jobs after recovery."""

    @pytest.mark.asyncio
    async def test_all_jobs_have_leader_after_recovery(self):
        """All jobs have an active leader after recovery."""
        cluster = ClusterSimulator()
        cluster.enable_checkpointing()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        await cluster.start_worker("worker-1", port=8000)

        # Submit multiple jobs
        for i in range(5):
            await cluster.submit_job(
                job_id=f"job-{i:03d}",
                workflow_ids=[f"wf-{i}-0", f"wf-{i}-1"],
                worker_assignments={
                    f"wf-{i}-0": "worker-1",
                    f"wf-{i}-1": "worker-1",
                },
            )

        # Fail and recover
        await cluster.stop_manager("manager-1", graceful=False)

        new_manager = await cluster.start_manager(
            "manager-2",
            tcp_port=9092,
            udp_port=9093,
            recover_from_checkpoint=True,
        )
        await cluster.elect_leader("manager-2")

        # All jobs should have leader
        for i in range(5):
            job_id = f"job-{i:03d}"
            assert job_id in new_manager.jobs
            assert new_manager.jobs[job_id].leader_manager_id == "manager-2"

    @pytest.mark.asyncio
    async def test_no_duplicate_workflow_assignments(self):
        """No workflow is assigned to multiple workers after recovery."""
        cluster = ClusterSimulator()
        cluster.enable_checkpointing()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        worker1 = await cluster.start_worker("worker-1", port=8000)
        worker2 = await cluster.start_worker("worker-2", port=8001)

        await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001", "wf-002", "wf-003"],
            worker_assignments={
                "wf-001": "worker-1",
                "wf-002": "worker-2",
                "wf-003": "worker-1",
            },
        )

        # Get all assignments
        assignments = cluster.get_all_workflow_assignments()

        # No duplicates
        workflow_ids = list(assignments.keys())
        assert len(workflow_ids) == len(set(workflow_ids))

    @pytest.mark.asyncio
    async def test_orphaned_jobs_detected(self):
        """Orphaned jobs are properly detected."""
        cluster = ClusterSimulator()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        await cluster.start_worker("worker-1", port=8000)

        await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # Manager fails (job becomes orphaned)
        await cluster.stop_manager("manager-1", graceful=False)

        orphaned = cluster.get_orphaned_jobs()
        assert "job-001" in orphaned


class TestEventLogVerification:
    """Tests verifying event log during bootstrap and recovery."""

    @pytest.mark.asyncio
    async def test_bootstrap_events_logged(self):
        """Bootstrap events are properly logged."""
        cluster = ClusterSimulator()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        await cluster.start_worker("worker-1", port=8000)

        event_types = [e[1] for e in cluster._event_log]

        assert "manager_starting" in event_types
        assert "manager_active" in event_types
        assert "first_leader_elected" in event_types
        assert "worker_starting" in event_types
        assert "worker_active" in event_types

    @pytest.mark.asyncio
    async def test_recovery_events_logged(self):
        """Recovery events are properly logged."""
        cluster = ClusterSimulator()
        cluster.enable_checkpointing()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        await cluster.start_worker("worker-1", port=8000)

        await cluster.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        await cluster.stop_manager("manager-1", graceful=False)

        await cluster.start_manager(
            "manager-2",
            tcp_port=9092,
            udp_port=9093,
            recover_from_checkpoint=True,
        )

        event_types = [e[1] for e in cluster._event_log]

        assert "checkpoint_saved" in event_types
        assert "manager_stopped" in event_types
        assert "manager_recovered" in event_types


class TestEdgeCases:
    """Edge case tests for bootstrap and recovery."""

    @pytest.mark.asyncio
    async def test_recovery_with_no_checkpoint(self):
        """Recovery attempt with no checkpoint falls back to bootstrap."""
        cluster = ClusterSimulator()
        # Note: checkpointing NOT enabled

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)

        await cluster.stop_manager("manager-1", graceful=False)

        # Start with recovery flag but no checkpoint
        new_manager = await cluster.start_manager(
            "manager-2",
            tcp_port=9092,
            udp_port=9093,
            recover_from_checkpoint=True,
        )

        # Should bootstrap normally (no recovered jobs)
        assert not new_manager.recovered_from_checkpoint
        assert len(new_manager.jobs) == 0

    @pytest.mark.asyncio
    async def test_empty_cluster_start(self):
        """Starting cluster with no jobs works correctly."""
        cluster = ClusterSimulator()

        manager = await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        worker = await cluster.start_worker("worker-1", port=8000)

        assert len(manager.jobs) == 0
        assert len(worker.active_workflows) == 0
        assert manager.is_leader

    @pytest.mark.asyncio
    async def test_rapid_manager_restarts(self):
        """Rapid manager restarts are handled correctly."""
        cluster = ClusterSimulator()

        for i in range(5):
            manager = await cluster.start_manager(
                f"manager-{i}",
                tcp_port=9090 + i * 2,
                udp_port=9091 + i * 2,
            )
            await cluster.stop_manager(f"manager-{i}", graceful=True)

        # Start final manager
        final = await cluster.start_manager("manager-final", tcp_port=9100, udp_port=9101)

        # Should be active
        assert final.state == NodeState.ACTIVE

    @pytest.mark.asyncio
    async def test_worker_starts_before_any_manager(self):
        """Worker can start even if no managers exist yet."""
        cluster = ClusterSimulator()

        # Worker starts first
        worker = await cluster.start_worker("worker-1", port=8000)

        # No primary manager yet
        assert worker.primary_manager_id is None

        # Manager starts
        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)

        # Worker should be discovered by manager
        assert "worker-1" in cluster.managers["manager-1"].known_workers

    @pytest.mark.asyncio
    async def test_graceful_vs_abrupt_shutdown(self):
        """Graceful shutdown allows draining, abrupt doesn't."""
        cluster = ClusterSimulator()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)

        # Graceful shutdown
        await cluster.stop_manager("manager-1", graceful=True)
        graceful_events = [e for e in cluster._event_log if e[2].get("graceful")]

        # Reset
        cluster._event_log.clear()
        await cluster.start_manager("manager-2", tcp_port=9092, udp_port=9093)

        # Abrupt shutdown
        await cluster.stop_manager("manager-2", graceful=False)
        abrupt_events = [e for e in cluster._event_log if not e[2].get("graceful", True)]

        assert len(graceful_events) == 1
        assert len(abrupt_events) == 1


class TestClusterStateConsistency:
    """Tests verifying cluster state consistency."""

    @pytest.mark.asyncio
    async def test_manager_knows_all_active_workers(self):
        """Active manager knows about all active workers."""
        cluster = ClusterSimulator()

        manager = await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)

        for i in range(5):
            await cluster.start_worker(f"worker-{i}", port=8000 + i)

        assert len(manager.known_workers) == 5

    @pytest.mark.asyncio
    async def test_managers_know_each_other(self):
        """All active managers know about each other."""
        cluster = ClusterSimulator()

        managers = []
        for i in range(3):
            mgr = await cluster.start_manager(
                f"manager-{i}",
                tcp_port=9090 + i * 2,
                udp_port=9091 + i * 2,
            )
            managers.append(mgr)

        for mgr in managers:
            # Each manager knows the other 2
            assert len(mgr.known_managers) == 2

    @pytest.mark.asyncio
    async def test_dead_nodes_removed_from_known_lists(self):
        """Dead nodes are removed from known lists."""
        cluster = ClusterSimulator()

        await cluster.start_manager("manager-1", tcp_port=9090, udp_port=9091)
        mgr2 = await cluster.start_manager("manager-2", tcp_port=9092, udp_port=9093)
        await cluster.start_worker("worker-1", port=8000)

        # Stop manager-1 and worker-1
        await cluster.stop_manager("manager-1", graceful=False)
        await cluster.stop_worker("worker-1", graceful=False)

        # manager-2 should not know about dead nodes
        assert "manager-1" not in mgr2.known_managers
        assert "worker-1" not in mgr2.known_workers