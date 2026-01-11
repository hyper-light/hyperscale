"""
End-to-end simulation tests for job distribution under node churn.

These tests simulate job submission and execution while nodes join/leave:
1. Worker dies mid-workflow, job is reassigned to another worker
2. Manager dies while coordinating job, new manager picks up from checkpoint
3. Rapid node membership changes while jobs are in flight
4. New workers join and receive job assignments from existing manager

Tests use mocks for all networking to avoid live server requirements.
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from typing import Any


# =============================================================================
# Shared Mock Infrastructure
# =============================================================================


@dataclass
class MockNodeId:
    """Mock node ID."""

    full: str
    short: str
    datacenter: str = "dc1"


@dataclass
class MockEnv:
    """Mock environment configuration."""

    RECOVERY_JITTER_MIN: float = 0.0
    RECOVERY_JITTER_MAX: float = 0.0
    DATACENTER_ID: str = "dc1"
    WORKER_ORPHAN_GRACE_PERIOD: float = 1.0
    WORKER_ORPHAN_CHECK_INTERVAL: float = 0.1


@dataclass
class MockLogger:
    """Mock logger."""

    _logs: list = field(default_factory=list)

    async def log(self, message: Any) -> None:
        self._logs.append(message)


@dataclass
class WorkflowSpec:
    """Specification for a workflow to be executed."""

    workflow_id: str
    job_id: str
    worker_id: str | None = None
    status: str = "pending"
    result: Any = None
    is_orphaned: bool = False
    orphan_timestamp: float | None = None


@dataclass
class JobSpec:
    """Specification for a job."""

    job_id: str
    workflow_specs: list[WorkflowSpec] = field(default_factory=list)
    leader_manager_id: str | None = None
    fence_token: int = 1


@dataclass
class WorkerState:
    """State of a simulated worker."""

    worker_id: str
    host: str
    port: int
    is_alive: bool = True
    active_workflows: dict[str, WorkflowSpec] = field(default_factory=dict)
    completed_workflows: list[str] = field(default_factory=list)
    orphaned_workflows: dict[str, float] = field(default_factory=dict)
    job_leaders: dict[str, tuple[str, int]] = field(default_factory=dict)
    fence_tokens: dict[str, int] = field(default_factory=dict)


@dataclass
class ManagerState:
    """State of a simulated manager."""

    manager_id: str
    host: str
    tcp_port: int
    udp_port: int
    is_alive: bool = True
    is_leader: bool = False
    jobs: dict[str, JobSpec] = field(default_factory=dict)
    known_workers: dict[str, WorkerState] = field(default_factory=dict)
    dead_managers: set[tuple[str, int]] = field(default_factory=set)


# =============================================================================
# Simulated Cluster with Churn Support
# =============================================================================


class ChurnSimulatedCluster:
    """
    Simulated cluster that supports node churn scenarios.

    Tracks job assignments, worker availability, and handles redistribution
    when nodes fail or join.
    """

    def __init__(self) -> None:
        self.managers: dict[str, ManagerState] = {}
        self.workers: dict[str, WorkerState] = {}
        self.jobs: dict[str, JobSpec] = {}

        self._current_leader_id: str | None = None
        self._event_log: list[tuple[float, str, dict]] = []
        self._workflow_assignments: dict[str, str] = {}  # workflow_id -> worker_id

    def log_event(self, event_type: str, details: dict) -> None:
        """Log a cluster event for later analysis."""
        self._event_log.append((time.monotonic(), event_type, details))

    def add_manager(
        self,
        manager_id: str,
        host: str = "127.0.0.1",
        tcp_port: int = 9090,
        udp_port: int = 9091,
    ) -> ManagerState:
        """Add a manager to the cluster."""
        manager = ManagerState(
            manager_id=manager_id,
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
        )
        self.managers[manager_id] = manager
        self.log_event("manager_joined", {"manager_id": manager_id})
        return manager

    def add_worker(
        self,
        worker_id: str,
        host: str = "127.0.0.1",
        port: int = 8000,
    ) -> WorkerState:
        """Add a worker to the cluster."""
        worker = WorkerState(
            worker_id=worker_id,
            host=host,
            port=port,
        )
        self.workers[worker_id] = worker

        # Register with all alive managers
        for manager in self.managers.values():
            if manager.is_alive:
                manager.known_workers[worker_id] = worker

        self.log_event("worker_joined", {"worker_id": worker_id})
        return worker

    def elect_leader(self, manager_id: str) -> None:
        """Elect a manager as the cluster leader."""
        # Step down old leader
        if self._current_leader_id:
            old_leader = self.managers.get(self._current_leader_id)
            if old_leader:
                old_leader.is_leader = False

        # Elect new leader
        self._current_leader_id = manager_id
        new_leader = self.managers[manager_id]
        new_leader.is_leader = True

        self.log_event("leader_elected", {"manager_id": manager_id})

    def get_leader(self) -> ManagerState | None:
        """Get the current cluster leader."""
        if self._current_leader_id:
            return self.managers.get(self._current_leader_id)
        return None

    def submit_job(self, job: JobSpec) -> None:
        """Submit a job to the cluster."""
        leader = self.get_leader()
        if not leader:
            raise RuntimeError("No leader elected")

        job.leader_manager_id = leader.manager_id
        self.jobs[job.job_id] = job
        leader.jobs[job.job_id] = job

        # Replicate to other managers
        for manager in self.managers.values():
            if manager.manager_id != leader.manager_id and manager.is_alive:
                manager.jobs[job.job_id] = JobSpec(
                    job_id=job.job_id,
                    workflow_specs=[],
                    leader_manager_id=leader.manager_id,
                    fence_token=job.fence_token,
                )

        self.log_event("job_submitted", {"job_id": job.job_id, "leader": leader.manager_id})

    def assign_workflow_to_worker(
        self,
        workflow: WorkflowSpec,
        worker_id: str,
    ) -> None:
        """Assign a workflow to a worker."""
        worker = self.workers.get(worker_id)
        if not worker or not worker.is_alive:
            raise RuntimeError(f"Worker {worker_id} not available")

        leader = self.get_leader()
        if not leader:
            raise RuntimeError("No leader")

        workflow.worker_id = worker_id
        workflow.status = "running"
        worker.active_workflows[workflow.workflow_id] = workflow
        worker.job_leaders[workflow.workflow_id] = (leader.host, leader.tcp_port)

        self._workflow_assignments[workflow.workflow_id] = worker_id

        self.log_event("workflow_assigned", {
            "workflow_id": workflow.workflow_id,
            "job_id": workflow.job_id,
            "worker_id": worker_id,
        })

    def fail_worker(self, worker_id: str) -> list[WorkflowSpec]:
        """Simulate worker failure. Returns orphaned workflows."""
        worker = self.workers.get(worker_id)
        if not worker:
            return []

        worker.is_alive = False
        orphaned = list(worker.active_workflows.values())

        # Mark workflows as orphaned
        for wf in orphaned:
            wf.is_orphaned = True
            wf.orphan_timestamp = time.monotonic()

        # Remove from manager's known workers
        for manager in self.managers.values():
            manager.known_workers.pop(worker_id, None)

        self.log_event("worker_failed", {
            "worker_id": worker_id,
            "orphaned_workflows": [wf.workflow_id for wf in orphaned],
        })

        return orphaned

    def recover_worker(self, worker_id: str) -> None:
        """Simulate worker recovery (rejoining the cluster)."""
        worker = self.workers.get(worker_id)
        if not worker:
            return

        worker.is_alive = True
        worker.active_workflows.clear()  # Lost state on restart
        worker.orphaned_workflows.clear()

        # Re-register with managers
        for manager in self.managers.values():
            if manager.is_alive:
                manager.known_workers[worker_id] = worker

        self.log_event("worker_recovered", {"worker_id": worker_id})

    def fail_manager(self, manager_id: str) -> None:
        """Simulate manager failure."""
        manager = self.managers.get(manager_id)
        if not manager:
            return

        manager.is_alive = False
        manager.is_leader = False

        # Mark manager as dead in other managers
        dead_addr = (manager.host, manager.tcp_port)
        for other_mgr in self.managers.values():
            if other_mgr.manager_id != manager_id and other_mgr.is_alive:
                other_mgr.dead_managers.add(dead_addr)

        self.log_event("manager_failed", {"manager_id": manager_id})

    def recover_manager(self, manager_id: str) -> None:
        """Simulate manager recovery."""
        manager = self.managers.get(manager_id)
        if not manager:
            return

        manager.is_alive = True
        manager.dead_managers.clear()

        # Remove from dead managers tracking in other managers
        recovered_addr = (manager.host, manager.tcp_port)
        for other_mgr in self.managers.values():
            if other_mgr.manager_id != manager_id:
                other_mgr.dead_managers.discard(recovered_addr)

        self.log_event("manager_recovered", {"manager_id": manager_id})

    def reassign_orphaned_workflow(
        self,
        workflow: WorkflowSpec,
        new_worker_id: str,
        new_fence_token: int,
    ) -> bool:
        """Reassign an orphaned workflow to a new worker."""
        new_worker = self.workers.get(new_worker_id)
        if not new_worker or not new_worker.is_alive:
            return False

        leader = self.get_leader()
        if not leader:
            return False

        # Update workflow
        workflow.worker_id = new_worker_id
        workflow.status = "running"
        workflow.is_orphaned = False
        workflow.orphan_timestamp = None

        # Update worker state
        new_worker.active_workflows[workflow.workflow_id] = workflow
        new_worker.job_leaders[workflow.workflow_id] = (leader.host, leader.tcp_port)
        new_worker.fence_tokens[workflow.job_id] = new_fence_token

        self._workflow_assignments[workflow.workflow_id] = new_worker_id

        self.log_event("workflow_reassigned", {
            "workflow_id": workflow.workflow_id,
            "new_worker_id": new_worker_id,
            "fence_token": new_fence_token,
        })

        return True

    def complete_workflow(self, workflow_id: str, result: Any = "success") -> None:
        """Mark a workflow as completed."""
        worker_id = self._workflow_assignments.get(workflow_id)
        if not worker_id:
            return

        worker = self.workers.get(worker_id)
        if not worker:
            return

        workflow = worker.active_workflows.pop(workflow_id, None)
        if workflow:
            workflow.status = "completed"
            workflow.result = result
            worker.completed_workflows.append(workflow_id)

        self.log_event("workflow_completed", {
            "workflow_id": workflow_id,
            "worker_id": worker_id,
        })

    def get_alive_workers(self) -> list[WorkerState]:
        """Get all alive workers."""
        return [w for w in self.workers.values() if w.is_alive]

    def get_alive_managers(self) -> list[ManagerState]:
        """Get all alive managers."""
        return [m for m in self.managers.values() if m.is_alive]


# =============================================================================
# Test Classes
# =============================================================================


class TestWorkerDiesMidWorkflow:
    """
    Test scenario: Worker dies mid-workflow, job is reassigned.

    Flow:
    1. Job submitted with workflow assigned to Worker-A
    2. Worker-A starts executing workflow
    3. Worker-A fails
    4. Manager detects failure, marks workflow as orphaned
    5. Workflow is reassigned to Worker-B
    6. Worker-B receives transfer with new fence token
    """

    @pytest.mark.asyncio
    async def test_single_workflow_reassignment(self):
        """Single workflow reassigned after worker failure."""
        cluster = ChurnSimulatedCluster()

        # Setup cluster
        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        worker_a = cluster.add_worker("worker-a", port=8000)
        worker_b = cluster.add_worker("worker-b", port=8001)

        cluster.elect_leader("manager-1")

        # Submit job
        job = JobSpec(
            job_id="job-001",
            workflow_specs=[
                WorkflowSpec(workflow_id="wf-001", job_id="job-001"),
            ],
        )
        cluster.submit_job(job)

        # Assign workflow to worker-a
        cluster.assign_workflow_to_worker(job.workflow_specs[0], "worker-a")

        assert "wf-001" in worker_a.active_workflows
        assert worker_a.active_workflows["wf-001"].status == "running"

        # Worker-A fails
        orphaned = cluster.fail_worker("worker-a")

        assert len(orphaned) == 1
        assert orphaned[0].workflow_id == "wf-001"
        assert orphaned[0].is_orphaned

        # Reassign to worker-b
        success = cluster.reassign_orphaned_workflow(
            orphaned[0],
            "worker-b",
            new_fence_token=2,
        )

        assert success
        assert "wf-001" in worker_b.active_workflows
        assert worker_b.fence_tokens["job-001"] == 2
        assert not worker_b.active_workflows["wf-001"].is_orphaned

    @pytest.mark.asyncio
    async def test_multiple_workflows_reassignment(self):
        """Multiple workflows from same worker reassigned to different workers."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        worker_a = cluster.add_worker("worker-a", port=8000)
        worker_b = cluster.add_worker("worker-b", port=8001)
        worker_c = cluster.add_worker("worker-c", port=8002)

        cluster.elect_leader("manager-1")

        # Submit job with multiple workflows
        job = JobSpec(
            job_id="job-001",
            workflow_specs=[
                WorkflowSpec(workflow_id=f"wf-{i:03d}", job_id="job-001")
                for i in range(5)
            ],
        )
        cluster.submit_job(job)

        # Assign all workflows to worker-a
        for wf in job.workflow_specs:
            cluster.assign_workflow_to_worker(wf, "worker-a")

        assert len(worker_a.active_workflows) == 5

        # Worker-A fails
        orphaned = cluster.fail_worker("worker-a")
        assert len(orphaned) == 5

        # Distribute workflows between worker-b and worker-c
        for i, wf in enumerate(orphaned):
            target = "worker-b" if i % 2 == 0 else "worker-c"
            cluster.reassign_orphaned_workflow(wf, target, new_fence_token=2)

        # Verify distribution
        assert len(worker_b.active_workflows) == 3
        assert len(worker_c.active_workflows) == 2

    @pytest.mark.asyncio
    async def test_no_available_worker_for_reassignment(self):
        """Reassignment fails when no workers are available."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        worker_a = cluster.add_worker("worker-a", port=8000)

        cluster.elect_leader("manager-1")

        job = JobSpec(
            job_id="job-001",
            workflow_specs=[WorkflowSpec(workflow_id="wf-001", job_id="job-001")],
        )
        cluster.submit_job(job)
        cluster.assign_workflow_to_worker(job.workflow_specs[0], "worker-a")

        # Worker-A fails (only worker)
        orphaned = cluster.fail_worker("worker-a")

        # Try to reassign to dead worker
        success = cluster.reassign_orphaned_workflow(
            orphaned[0],
            "worker-a",  # Dead worker
            new_fence_token=2,
        )

        assert not success
        assert orphaned[0].is_orphaned  # Still orphaned

    @pytest.mark.asyncio
    async def test_workflow_completes_after_reassignment(self):
        """Workflow successfully completes after being reassigned."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        worker_a = cluster.add_worker("worker-a", port=8000)
        worker_b = cluster.add_worker("worker-b", port=8001)

        cluster.elect_leader("manager-1")

        job = JobSpec(
            job_id="job-001",
            workflow_specs=[WorkflowSpec(workflow_id="wf-001", job_id="job-001")],
        )
        cluster.submit_job(job)
        cluster.assign_workflow_to_worker(job.workflow_specs[0], "worker-a")

        # Fail and reassign
        orphaned = cluster.fail_worker("worker-a")
        cluster.reassign_orphaned_workflow(orphaned[0], "worker-b", new_fence_token=2)

        # Complete the workflow
        cluster.complete_workflow("wf-001", result="final_result")

        assert "wf-001" not in worker_b.active_workflows
        assert "wf-001" in worker_b.completed_workflows


class TestManagerDiesWhileCoordinating:
    """
    Test scenario: Manager dies while coordinating job.

    Flow:
    1. Manager-A is job leader, coordinating workflows
    2. Manager-A fails
    3. Manager-B becomes new leader
    4. Manager-B takes over job coordination
    5. Workers receive transfer notifications
    """

    @pytest.mark.asyncio
    async def test_job_coordination_handoff(self):
        """New manager takes over job coordination after leader failure."""
        cluster = ChurnSimulatedCluster()

        manager_a = cluster.add_manager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = cluster.add_manager("manager-b", tcp_port=9092, udp_port=9093)
        worker = cluster.add_worker("worker-1", port=8000)

        cluster.elect_leader("manager-a")

        # Submit job led by manager-a
        job = JobSpec(
            job_id="job-001",
            workflow_specs=[WorkflowSpec(workflow_id="wf-001", job_id="job-001")],
            fence_token=1,
        )
        cluster.submit_job(job)
        cluster.assign_workflow_to_worker(job.workflow_specs[0], "worker-1")

        assert job.leader_manager_id == "manager-a"

        # Manager-A fails
        cluster.fail_manager("manager-a")

        # Manager-B becomes leader
        cluster.elect_leader("manager-b")

        # Manager-B should have the job tracked
        assert "job-001" in manager_b.jobs

        # Simulate takeover: update job leadership
        job.leader_manager_id = "manager-b"
        job.fence_token = 2
        manager_b.jobs["job-001"] = job

        assert job.leader_manager_id == "manager-b"
        assert job.fence_token == 2

    @pytest.mark.asyncio
    async def test_multiple_jobs_during_manager_failure(self):
        """Multiple jobs correctly transferred during manager failure."""
        cluster = ChurnSimulatedCluster()

        manager_a = cluster.add_manager("manager-a", tcp_port=9090, udp_port=9091)
        manager_b = cluster.add_manager("manager-b", tcp_port=9092, udp_port=9093)

        workers = [
            cluster.add_worker(f"worker-{i}", port=8000 + i)
            for i in range(3)
        ]

        cluster.elect_leader("manager-a")

        # Submit multiple jobs
        jobs = []
        for i in range(3):
            job = JobSpec(
                job_id=f"job-{i:03d}",
                workflow_specs=[
                    WorkflowSpec(workflow_id=f"wf-{i}-{j}", job_id=f"job-{i:03d}")
                    for j in range(2)
                ],
                fence_token=1,
            )
            cluster.submit_job(job)
            jobs.append(job)

            # Assign workflows
            for j, wf in enumerate(job.workflow_specs):
                cluster.assign_workflow_to_worker(wf, f"worker-{j % 3}")

        # Manager-A fails
        cluster.fail_manager("manager-a")
        cluster.elect_leader("manager-b")

        # All jobs should be tracked by manager-b
        for job in jobs:
            assert job.job_id in manager_b.jobs

        # Simulate takeover
        for job in jobs:
            job.leader_manager_id = "manager-b"
            job.fence_token = 2


class TestRapidMembershipChanges:
    """
    Test scenario: Rapid node membership changes while jobs are in flight.

    Flow:
    1. Jobs are running on multiple workers
    2. Workers rapidly join and leave
    3. Jobs are correctly redistributed
    4. No workflows are lost or duplicated
    """

    @pytest.mark.asyncio
    async def test_rapid_worker_churn(self):
        """Jobs survive rapid worker join/leave cycles."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.elect_leader("manager-1")

        # Create initial workers
        workers = [
            cluster.add_worker(f"worker-{i}", port=8000 + i)
            for i in range(5)
        ]

        # Submit job with many workflows
        job = JobSpec(
            job_id="job-001",
            workflow_specs=[
                WorkflowSpec(workflow_id=f"wf-{i:03d}", job_id="job-001")
                for i in range(10)
            ],
        )
        cluster.submit_job(job)

        # Distribute workflows
        for i, wf in enumerate(job.workflow_specs):
            cluster.assign_workflow_to_worker(wf, f"worker-{i % 5}")

        # Rapid churn: fail and add workers
        for cycle in range(3):
            # Fail worker-{cycle}
            orphaned = cluster.fail_worker(f"worker-{cycle}")

            # Add replacement worker
            replacement = cluster.add_worker(f"worker-replacement-{cycle}", port=8100 + cycle)

            # Reassign orphaned workflows
            for wf in orphaned:
                cluster.reassign_orphaned_workflow(
                    wf,
                    f"worker-replacement-{cycle}",
                    new_fence_token=cycle + 2,
                )

        # Verify no workflows lost
        total_active = sum(
            len(w.active_workflows)
            for w in cluster.workers.values()
            if w.is_alive
        )
        assert total_active == 10

    @pytest.mark.asyncio
    async def test_simultaneous_worker_failures(self):
        """Multiple workers fail simultaneously."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.elect_leader("manager-1")

        # Create workers
        workers = [
            cluster.add_worker(f"worker-{i}", port=8000 + i)
            for i in range(6)
        ]

        # Submit job
        job = JobSpec(
            job_id="job-001",
            workflow_specs=[
                WorkflowSpec(workflow_id=f"wf-{i:03d}", job_id="job-001")
                for i in range(6)
            ],
        )
        cluster.submit_job(job)

        # One workflow per worker
        for i, wf in enumerate(job.workflow_specs):
            cluster.assign_workflow_to_worker(wf, f"worker-{i}")

        # Fail half the workers simultaneously
        all_orphaned = []
        for i in range(3):
            orphaned = cluster.fail_worker(f"worker-{i}")
            all_orphaned.extend(orphaned)

        assert len(all_orphaned) == 3

        # Redistribute to surviving workers
        surviving_workers = ["worker-3", "worker-4", "worker-5"]
        for i, wf in enumerate(all_orphaned):
            target = surviving_workers[i % len(surviving_workers)]
            cluster.reassign_orphaned_workflow(wf, target, new_fence_token=2)

        # Verify all workflows are assigned
        alive_workers = cluster.get_alive_workers()
        total_workflows = sum(len(w.active_workflows) for w in alive_workers)
        assert total_workflows == 6

    @pytest.mark.asyncio
    async def test_worker_rejoins_after_failure(self):
        """Worker rejoins cluster and receives new assignments."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.elect_leader("manager-1")

        worker_a = cluster.add_worker("worker-a", port=8000)
        worker_b = cluster.add_worker("worker-b", port=8001)

        # Initial job assignment
        job1 = JobSpec(
            job_id="job-001",
            workflow_specs=[WorkflowSpec(workflow_id="wf-001", job_id="job-001")],
        )
        cluster.submit_job(job1)
        cluster.assign_workflow_to_worker(job1.workflow_specs[0], "worker-a")

        # Worker-A fails, workflow moved to worker-b
        orphaned = cluster.fail_worker("worker-a")
        cluster.reassign_orphaned_workflow(orphaned[0], "worker-b", new_fence_token=2)

        # Worker-A recovers
        cluster.recover_worker("worker-a")

        # Worker-A should be empty (lost state on restart)
        assert len(worker_a.active_workflows) == 0
        assert worker_a.is_alive

        # New job can be assigned to recovered worker
        job2 = JobSpec(
            job_id="job-002",
            workflow_specs=[WorkflowSpec(workflow_id="wf-002", job_id="job-002")],
        )
        cluster.submit_job(job2)
        cluster.assign_workflow_to_worker(job2.workflow_specs[0], "worker-a")

        assert "wf-002" in worker_a.active_workflows


class TestNewWorkersJoinAndReceiveAssignments:
    """
    Test scenario: New workers join and receive job assignments.

    Flow:
    1. Cluster running with existing workers
    2. New workers join
    3. New jobs are load-balanced to include new workers
    4. Existing jobs can be partially migrated to new workers
    """

    @pytest.mark.asyncio
    async def test_new_worker_receives_new_job(self):
        """Newly joined worker receives assignment for new job."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.elect_leader("manager-1")

        # Existing worker with job
        existing_worker = cluster.add_worker("worker-existing", port=8000)
        job1 = JobSpec(
            job_id="job-001",
            workflow_specs=[WorkflowSpec(workflow_id="wf-001", job_id="job-001")],
        )
        cluster.submit_job(job1)
        cluster.assign_workflow_to_worker(job1.workflow_specs[0], "worker-existing")

        # New worker joins
        new_worker = cluster.add_worker("worker-new", port=8001)
        assert new_worker.is_alive
        assert "worker-new" in manager.known_workers

        # New job assigned to new worker
        job2 = JobSpec(
            job_id="job-002",
            workflow_specs=[WorkflowSpec(workflow_id="wf-002", job_id="job-002")],
        )
        cluster.submit_job(job2)
        cluster.assign_workflow_to_worker(job2.workflow_specs[0], "worker-new")

        assert "wf-002" in new_worker.active_workflows
        assert "wf-001" in existing_worker.active_workflows

    @pytest.mark.asyncio
    async def test_load_balancing_with_new_workers(self):
        """Jobs are load-balanced across existing and new workers."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.elect_leader("manager-1")

        # Start with 2 workers
        worker_1 = cluster.add_worker("worker-1", port=8000)
        worker_2 = cluster.add_worker("worker-2", port=8001)

        # Initial load
        job1 = JobSpec(
            job_id="job-001",
            workflow_specs=[
                WorkflowSpec(workflow_id=f"wf-1-{i}", job_id="job-001")
                for i in range(4)
            ],
        )
        cluster.submit_job(job1)

        for i, wf in enumerate(job1.workflow_specs):
            target = "worker-1" if i % 2 == 0 else "worker-2"
            cluster.assign_workflow_to_worker(wf, target)

        # Both workers have 2 workflows
        assert len(worker_1.active_workflows) == 2
        assert len(worker_2.active_workflows) == 2

        # Add 2 new workers
        worker_3 = cluster.add_worker("worker-3", port=8002)
        worker_4 = cluster.add_worker("worker-4", port=8003)

        # New job distributed across all 4 workers
        job2 = JobSpec(
            job_id="job-002",
            workflow_specs=[
                WorkflowSpec(workflow_id=f"wf-2-{i}", job_id="job-002")
                for i in range(4)
            ],
        )
        cluster.submit_job(job2)

        worker_ids = ["worker-1", "worker-2", "worker-3", "worker-4"]
        for i, wf in enumerate(job2.workflow_specs):
            cluster.assign_workflow_to_worker(wf, worker_ids[i])

        # Verify distribution
        assert len(worker_1.active_workflows) == 3
        assert len(worker_2.active_workflows) == 3
        assert len(worker_3.active_workflows) == 1
        assert len(worker_4.active_workflows) == 1

    @pytest.mark.asyncio
    async def test_scaling_out_during_high_load(self):
        """New workers join during high load and help process backlog."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.elect_leader("manager-1")

        # Start with 1 overloaded worker
        worker_1 = cluster.add_worker("worker-1", port=8000)

        # Submit large job
        job = JobSpec(
            job_id="job-001",
            workflow_specs=[
                WorkflowSpec(workflow_id=f"wf-{i:03d}", job_id="job-001")
                for i in range(20)
            ],
        )
        cluster.submit_job(job)

        # All workflows assigned to single worker
        for wf in job.workflow_specs:
            cluster.assign_workflow_to_worker(wf, "worker-1")

        assert len(worker_1.active_workflows) == 20

        # Scale out: add 4 more workers
        new_workers = [
            cluster.add_worker(f"worker-{i}", port=8000 + i)
            for i in range(2, 6)
        ]

        # Simulate load redistribution:
        # Move some workflows from worker-1 to new workers
        workflows_to_move = list(worker_1.active_workflows.values())[:16]
        for i, wf in enumerate(workflows_to_move):
            # Remove from worker-1
            del worker_1.active_workflows[wf.workflow_id]

            # Assign to new worker
            target_idx = i % 4
            cluster.reassign_orphaned_workflow(
                wf,
                f"worker-{target_idx + 2}",
                new_fence_token=2,
            )

        # Verify balanced distribution
        assert len(worker_1.active_workflows) == 4
        for nw in new_workers:
            assert len(nw.active_workflows) == 4


class TestEventLogAnalysis:
    """Tests that verify event logging during churn scenarios."""

    @pytest.mark.asyncio
    async def test_event_log_captures_all_events(self):
        """Event log captures all cluster events in order."""
        cluster = ChurnSimulatedCluster()

        # Setup
        cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.add_worker("worker-1", port=8000)
        cluster.elect_leader("manager-1")

        job = JobSpec(
            job_id="job-001",
            workflow_specs=[WorkflowSpec(workflow_id="wf-001", job_id="job-001")],
        )
        cluster.submit_job(job)
        cluster.assign_workflow_to_worker(job.workflow_specs[0], "worker-1")
        cluster.fail_worker("worker-1")

        # Verify event log
        event_types = [event[1] for event in cluster._event_log]

        assert "manager_joined" in event_types
        assert "worker_joined" in event_types
        assert "leader_elected" in event_types
        assert "job_submitted" in event_types
        assert "workflow_assigned" in event_types
        assert "worker_failed" in event_types

    @pytest.mark.asyncio
    async def test_event_log_timestamps_are_ordered(self):
        """Event timestamps are monotonically increasing."""
        cluster = ChurnSimulatedCluster()

        cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.add_worker("worker-1", port=8000)
        cluster.add_worker("worker-2", port=8001)
        cluster.elect_leader("manager-1")

        job = JobSpec(
            job_id="job-001",
            workflow_specs=[
                WorkflowSpec(workflow_id=f"wf-{i}", job_id="job-001")
                for i in range(5)
            ],
        )
        cluster.submit_job(job)

        for wf in job.workflow_specs:
            cluster.assign_workflow_to_worker(wf, "worker-1")

        # Verify timestamps are ordered
        timestamps = [event[0] for event in cluster._event_log]
        for i in range(1, len(timestamps)):
            assert timestamps[i] >= timestamps[i - 1]


class TestInvariantVerification:
    """Tests that verify system invariants are maintained during churn."""

    @pytest.mark.asyncio
    async def test_no_duplicate_workflow_assignments(self):
        """Each workflow is assigned to at most one worker at a time."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.elect_leader("manager-1")

        workers = [
            cluster.add_worker(f"worker-{i}", port=8000 + i)
            for i in range(3)
        ]

        job = JobSpec(
            job_id="job-001",
            workflow_specs=[
                WorkflowSpec(workflow_id=f"wf-{i:03d}", job_id="job-001")
                for i in range(10)
            ],
        )
        cluster.submit_job(job)

        for i, wf in enumerate(job.workflow_specs):
            cluster.assign_workflow_to_worker(wf, f"worker-{i % 3}")

        # Churn: fail worker-0, reassign to worker-1
        orphaned = cluster.fail_worker("worker-0")
        for wf in orphaned:
            cluster.reassign_orphaned_workflow(wf, "worker-1", new_fence_token=2)

        # Verify no duplicates
        all_workflow_ids: list[str] = []
        for worker in cluster.workers.values():
            if worker.is_alive:
                all_workflow_ids.extend(worker.active_workflows.keys())

        # No duplicates
        assert len(all_workflow_ids) == len(set(all_workflow_ids))

    @pytest.mark.asyncio
    async def test_orphaned_workflows_eventually_reassigned_or_cancelled(self):
        """All orphaned workflows are handled (reassigned or marked cancelled)."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.elect_leader("manager-1")

        worker_a = cluster.add_worker("worker-a", port=8000)
        worker_b = cluster.add_worker("worker-b", port=8001)

        job = JobSpec(
            job_id="job-001",
            workflow_specs=[
                WorkflowSpec(workflow_id=f"wf-{i:03d}", job_id="job-001")
                for i in range(5)
            ],
        )
        cluster.submit_job(job)

        for wf in job.workflow_specs:
            cluster.assign_workflow_to_worker(wf, "worker-a")

        # Fail worker-a
        orphaned = cluster.fail_worker("worker-a")

        # All orphaned workflows are explicitly handled
        reassigned_count = 0
        for wf in orphaned:
            if cluster.reassign_orphaned_workflow(wf, "worker-b", new_fence_token=2):
                reassigned_count += 1

        assert reassigned_count == 5
        assert len(worker_b.active_workflows) == 5

    @pytest.mark.asyncio
    async def test_fence_token_always_increases(self):
        """Fence tokens monotonically increase across reassignments."""
        cluster = ChurnSimulatedCluster()

        manager = cluster.add_manager("manager-1", tcp_port=9090, udp_port=9091)
        cluster.elect_leader("manager-1")

        workers = [
            cluster.add_worker(f"worker-{i}", port=8000 + i)
            for i in range(3)
        ]

        job = JobSpec(
            job_id="job-001",
            workflow_specs=[WorkflowSpec(workflow_id="wf-001", job_id="job-001")],
            fence_token=1,
        )
        cluster.submit_job(job)
        cluster.assign_workflow_to_worker(job.workflow_specs[0], "worker-0")
        workers[0].fence_tokens["job-001"] = 1

        # Track fence tokens through multiple reassignments
        expected_token = 1
        current_worker_idx = 0

        for reassignment in range(5):
            # Fail current worker
            orphaned = cluster.fail_worker(f"worker-{current_worker_idx}")

            # Move to next worker
            next_worker_idx = (current_worker_idx + 1) % 3
            cluster.recover_worker(f"worker-{next_worker_idx}")

            expected_token += 1
            cluster.reassign_orphaned_workflow(
                orphaned[0],
                f"worker-{next_worker_idx}",
                new_fence_token=expected_token,
            )

            # Verify token increased
            assert workers[next_worker_idx].fence_tokens["job-001"] == expected_token

            current_worker_idx = next_worker_idx