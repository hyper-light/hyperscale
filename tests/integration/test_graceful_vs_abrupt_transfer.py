"""
End-to-end simulation tests comparing graceful vs abrupt leadership transfers.

These tests compare the two transfer modes:
1. Graceful handoff: old leader coordinates with new leader before stepping down
2. Abrupt failure: leader crashes, new leader must reconstruct state from workers
3. Mixed: graceful starts but old leader dies mid-transfer
4. Verify workflow progress is preserved in all cases

Tests use mocks for all networking to avoid live server requirements.
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from typing import Any
from enum import Enum


# =============================================================================
# Mock Infrastructure
# =============================================================================


class TransferMode(Enum):
    """Mode of leadership transfer."""

    GRACEFUL = "graceful"  # Planned handoff with coordination
    ABRUPT = "abrupt"  # Crash/failure, no coordination


@dataclass
class WorkflowProgress:
    """Tracks workflow execution progress."""

    workflow_id: str
    job_id: str
    completed_count: int = 0
    total_count: int = 100
    status: str = "running"
    last_checkpoint: float = 0.0
    checkpointed_at_count: int = 0

    @property
    def progress_percent(self) -> float:
        return (self.completed_count / self.total_count) * 100 if self.total_count > 0 else 0

    def checkpoint(self) -> None:
        """Create a checkpoint of current progress."""
        self.checkpointed_at_count = self.completed_count
        self.last_checkpoint = time.monotonic()


@dataclass
class TransferState:
    """State transferred during leadership handoff."""

    job_id: str
    workflow_states: dict[str, WorkflowProgress]
    fence_token: int
    old_leader_id: str
    new_leader_id: str
    transfer_mode: TransferMode
    transfer_started: float = field(default_factory=time.monotonic)
    transfer_completed: float | None = None


@dataclass
class LeaderState:
    """State maintained by a leader manager."""

    manager_id: str
    jobs: dict[str, "JobState"] = field(default_factory=dict)
    is_leader: bool = False
    fence_tokens: dict[str, int] = field(default_factory=dict)


@dataclass
class JobState:
    """Job state maintained by leader."""

    job_id: str
    workflows: dict[str, WorkflowProgress] = field(default_factory=dict)
    worker_assignments: dict[str, str] = field(default_factory=dict)  # workflow_id -> worker_id


@dataclass
class WorkerState:
    """State maintained by a worker."""

    worker_id: str
    active_workflows: dict[str, WorkflowProgress] = field(default_factory=dict)
    job_leaders: dict[str, tuple[str, int]] = field(default_factory=dict)  # job_id -> (host, port)
    fence_tokens: dict[str, int] = field(default_factory=dict)
    orphaned_workflows: set[str] = field(default_factory=set)


# =============================================================================
# Transfer Coordinator
# =============================================================================


class TransferCoordinator:
    """
    Coordinates leadership transfers between managers.

    Supports both graceful and abrupt transfer modes.
    """

    def __init__(self) -> None:
        self.managers: dict[str, LeaderState] = {}
        self.workers: dict[str, WorkerState] = {}
        self._transfer_history: list[TransferState] = []
        self._current_leader_id: str | None = None

    def add_manager(self, manager_id: str) -> LeaderState:
        """Add a manager to the cluster."""
        state = LeaderState(manager_id=manager_id)
        self.managers[manager_id] = state
        return state

    def add_worker(self, worker_id: str) -> WorkerState:
        """Add a worker to the cluster."""
        state = WorkerState(worker_id=worker_id)
        self.workers[worker_id] = state
        return state

    def elect_leader(self, manager_id: str) -> None:
        """Elect a manager as leader."""
        if self._current_leader_id:
            old_leader = self.managers.get(self._current_leader_id)
            if old_leader:
                old_leader.is_leader = False

        self._current_leader_id = manager_id
        self.managers[manager_id].is_leader = True

    def submit_job(
        self,
        job_id: str,
        workflow_ids: list[str],
        worker_assignments: dict[str, str],
    ) -> None:
        """Submit a job to the current leader."""
        leader = self.managers.get(self._current_leader_id)
        if not leader:
            raise RuntimeError("No leader")

        job = JobState(
            job_id=job_id,
            workflows={
                wf_id: WorkflowProgress(workflow_id=wf_id, job_id=job_id)
                for wf_id in workflow_ids
            },
            worker_assignments=worker_assignments,
        )
        leader.jobs[job_id] = job
        leader.fence_tokens[job_id] = 1

        # Assign to workers
        for wf_id, worker_id in worker_assignments.items():
            worker = self.workers.get(worker_id)
            if worker:
                worker.active_workflows[wf_id] = job.workflows[wf_id]
                worker.job_leaders[job_id] = ("127.0.0.1", 9090)
                worker.fence_tokens[job_id] = 1

    async def graceful_transfer(
        self,
        old_leader_id: str,
        new_leader_id: str,
        job_id: str,
    ) -> TransferState:
        """
        Perform a graceful leadership transfer.

        1. Old leader pauses new work acceptance
        2. Old leader sends current state to new leader
        3. New leader takes over
        4. Old leader steps down
        5. Workers are notified of new leader
        """
        old_leader = self.managers[old_leader_id]
        new_leader = self.managers[new_leader_id]
        job = old_leader.jobs.get(job_id)

        if not job:
            raise RuntimeError(f"Job {job_id} not found on {old_leader_id}")

        # Create transfer state
        transfer = TransferState(
            job_id=job_id,
            workflow_states=dict(job.workflows),
            fence_token=old_leader.fence_tokens.get(job_id, 0) + 1,
            old_leader_id=old_leader_id,
            new_leader_id=new_leader_id,
            transfer_mode=TransferMode.GRACEFUL,
        )

        # Simulate coordination delay
        await asyncio.sleep(0.01)

        # Transfer job to new leader
        new_leader.jobs[job_id] = JobState(
            job_id=job_id,
            workflows=dict(job.workflows),  # Copy progress state
            worker_assignments=dict(job.worker_assignments),
        )
        new_leader.fence_tokens[job_id] = transfer.fence_token

        # Notify workers
        for wf_id, worker_id in job.worker_assignments.items():
            worker = self.workers.get(worker_id)
            if worker:
                worker.job_leaders[job_id] = ("127.0.0.1", 9092)  # New leader addr
                worker.fence_tokens[job_id] = transfer.fence_token
                worker.orphaned_workflows.discard(wf_id)

        # Step down old leader
        old_leader.is_leader = False
        del old_leader.jobs[job_id]

        # Complete transfer
        new_leader.is_leader = True
        self._current_leader_id = new_leader_id

        transfer.transfer_completed = time.monotonic()
        self._transfer_history.append(transfer)

        return transfer

    async def abrupt_transfer(
        self,
        failed_leader_id: str,
        new_leader_id: str,
        job_id: str,
    ) -> TransferState:
        """
        Perform an abrupt transfer after leader failure.

        1. Old leader is marked dead (no coordination possible)
        2. New leader reconstructs state from workers
        3. New leader takes over
        4. Workers are notified of new leader
        """
        old_leader = self.managers[failed_leader_id]
        new_leader = self.managers[new_leader_id]
        job = old_leader.jobs.get(job_id)

        if not job:
            raise RuntimeError(f"Job {job_id} not found on {failed_leader_id}")

        # Mark old leader as dead
        old_leader.is_leader = False

        # Mark workers' workflows as orphaned
        for wf_id, worker_id in job.worker_assignments.items():
            worker = self.workers.get(worker_id)
            if worker:
                worker.orphaned_workflows.add(wf_id)

        # Reconstruct state from workers (with potential data loss)
        reconstructed_workflows = {}
        for wf_id, worker_id in job.worker_assignments.items():
            worker = self.workers.get(worker_id)
            if worker and wf_id in worker.active_workflows:
                # Use worker's last known state (may be behind leader's state)
                reconstructed_workflows[wf_id] = worker.active_workflows[wf_id]

        # Create transfer state
        old_token = old_leader.fence_tokens.get(job_id, 0)
        transfer = TransferState(
            job_id=job_id,
            workflow_states=reconstructed_workflows,
            fence_token=old_token + 1,
            old_leader_id=failed_leader_id,
            new_leader_id=new_leader_id,
            transfer_mode=TransferMode.ABRUPT,
        )

        # New leader takes over with reconstructed state
        new_leader.jobs[job_id] = JobState(
            job_id=job_id,
            workflows=reconstructed_workflows,
            worker_assignments=dict(job.worker_assignments),
        )
        new_leader.fence_tokens[job_id] = transfer.fence_token

        # Notify workers
        for wf_id, worker_id in job.worker_assignments.items():
            worker = self.workers.get(worker_id)
            if worker:
                worker.job_leaders[job_id] = ("127.0.0.1", 9092)
                worker.fence_tokens[job_id] = transfer.fence_token
                worker.orphaned_workflows.discard(wf_id)

        # Complete transfer
        new_leader.is_leader = True
        self._current_leader_id = new_leader_id
        del old_leader.jobs[job_id]

        transfer.transfer_completed = time.monotonic()
        self._transfer_history.append(transfer)

        return transfer

    async def interrupted_graceful_transfer(
        self,
        old_leader_id: str,
        new_leader_id: str,
        job_id: str,
        interrupt_point: float,  # 0.0 to 1.0, when to interrupt
    ) -> TransferState:
        """
        Graceful transfer that gets interrupted by old leader failure.

        Simulates partial transfer where old leader crashes mid-handoff.
        """
        old_leader = self.managers[old_leader_id]
        new_leader = self.managers[new_leader_id]
        job = old_leader.jobs.get(job_id)

        if not job:
            raise RuntimeError(f"Job {job_id} not found on {old_leader_id}")

        # Start graceful transfer
        transfer = TransferState(
            job_id=job_id,
            workflow_states=dict(job.workflows),
            fence_token=old_leader.fence_tokens.get(job_id, 0) + 1,
            old_leader_id=old_leader_id,
            new_leader_id=new_leader_id,
            transfer_mode=TransferMode.GRACEFUL,  # Started graceful
        )

        # Partial transfer based on interrupt_point
        workflows_to_transfer = list(job.workflows.items())
        num_transferred = int(len(workflows_to_transfer) * interrupt_point)

        # Transfer some workflows
        partial_workflows = dict(workflows_to_transfer[:num_transferred])

        # Old leader crashes at interrupt point
        old_leader.is_leader = False

        # Mark remaining workflows as orphaned on workers
        for wf_id, worker_id in list(job.worker_assignments.items())[num_transferred:]:
            worker = self.workers.get(worker_id)
            if worker:
                worker.orphaned_workflows.add(wf_id)

        # New leader has partial state, must recover rest from workers
        for wf_id, worker_id in list(job.worker_assignments.items())[num_transferred:]:
            worker = self.workers.get(worker_id)
            if worker and wf_id in worker.active_workflows:
                partial_workflows[wf_id] = worker.active_workflows[wf_id]

        # Complete with combined state
        new_leader.jobs[job_id] = JobState(
            job_id=job_id,
            workflows=partial_workflows,
            worker_assignments=dict(job.worker_assignments),
        )
        new_leader.fence_tokens[job_id] = transfer.fence_token

        # Notify all workers
        for wf_id, worker_id in job.worker_assignments.items():
            worker = self.workers.get(worker_id)
            if worker:
                worker.job_leaders[job_id] = ("127.0.0.1", 9092)
                worker.fence_tokens[job_id] = transfer.fence_token
                worker.orphaned_workflows.discard(wf_id)

        new_leader.is_leader = True
        self._current_leader_id = new_leader_id
        del old_leader.jobs[job_id]

        transfer.workflow_states = partial_workflows
        transfer.transfer_completed = time.monotonic()
        self._transfer_history.append(transfer)

        return transfer

    def get_leader(self) -> LeaderState | None:
        if self._current_leader_id:
            return self.managers.get(self._current_leader_id)
        return None


# =============================================================================
# Test Classes
# =============================================================================


class TestGracefulTransfer:
    """Tests for graceful (planned) leadership transfers."""

    @pytest.mark.asyncio
    async def test_graceful_preserves_all_progress(self):
        """Graceful transfer preserves all workflow progress."""
        coordinator = TransferCoordinator()

        manager_a = coordinator.add_manager("manager-a")
        manager_b = coordinator.add_manager("manager-b")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # Simulate progress
        worker.active_workflows["wf-001"].completed_count = 50

        # Graceful transfer
        transfer = await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")

        # Verify progress preserved
        assert transfer.transfer_mode == TransferMode.GRACEFUL
        assert "wf-001" in transfer.workflow_states
        assert transfer.workflow_states["wf-001"].completed_count == 50

        # New leader has the progress
        assert manager_b.jobs["job-001"].workflows["wf-001"].completed_count == 50

    @pytest.mark.asyncio
    async def test_graceful_updates_fence_token(self):
        """Graceful transfer increments fence token."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        initial_token = worker.fence_tokens["job-001"]

        transfer = await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")

        assert transfer.fence_token == initial_token + 1
        assert worker.fence_tokens["job-001"] == initial_token + 1

    @pytest.mark.asyncio
    async def test_graceful_clears_orphan_status(self):
        """Graceful transfer ensures workflows are not orphaned."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # Pre-mark as orphaned (shouldn't happen in graceful, but test clearing)
        worker.orphaned_workflows.add("wf-001")

        await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")

        assert "wf-001" not in worker.orphaned_workflows

    @pytest.mark.asyncio
    async def test_graceful_multiple_workflows(self):
        """Graceful transfer handles multiple workflows correctly."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")

        workers = [coordinator.add_worker(f"worker-{i}") for i in range(3)]

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001", "wf-002", "wf-003"],
            worker_assignments={
                "wf-001": "worker-0",
                "wf-002": "worker-1",
                "wf-003": "worker-2",
            },
        )

        # Different progress on each
        workers[0].active_workflows["wf-001"].completed_count = 30
        workers[1].active_workflows["wf-002"].completed_count = 60
        workers[2].active_workflows["wf-003"].completed_count = 90

        transfer = await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")

        # All progress preserved
        assert transfer.workflow_states["wf-001"].completed_count == 30
        assert transfer.workflow_states["wf-002"].completed_count == 60
        assert transfer.workflow_states["wf-003"].completed_count == 90


class TestAbruptTransfer:
    """Tests for abrupt (failure) leadership transfers."""

    @pytest.mark.asyncio
    async def test_abrupt_reconstructs_from_workers(self):
        """Abrupt transfer reconstructs state from workers."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # Worker has progress
        worker.active_workflows["wf-001"].completed_count = 50

        # Abrupt transfer (leader crash)
        transfer = await coordinator.abrupt_transfer("manager-a", "manager-b", "job-001")

        assert transfer.transfer_mode == TransferMode.ABRUPT
        # Progress recovered from worker
        assert transfer.workflow_states["wf-001"].completed_count == 50

    @pytest.mark.asyncio
    async def test_abrupt_marks_orphaned_then_clears(self):
        """Abrupt transfer temporarily marks workflows orphaned, then clears."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        await coordinator.abrupt_transfer("manager-a", "manager-b", "job-001")

        # After transfer completes, orphan status cleared
        assert "wf-001" not in worker.orphaned_workflows

    @pytest.mark.asyncio
    async def test_abrupt_increments_fence_token(self):
        """Abrupt transfer also increments fence token."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        initial_token = worker.fence_tokens["job-001"]

        transfer = await coordinator.abrupt_transfer("manager-a", "manager-b", "job-001")

        assert transfer.fence_token == initial_token + 1

    @pytest.mark.asyncio
    async def test_abrupt_handles_missing_worker_data(self):
        """Abrupt transfer handles case where worker data is missing."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # Remove workflow from worker (simulating data loss)
        del worker.active_workflows["wf-001"]

        transfer = await coordinator.abrupt_transfer("manager-a", "manager-b", "job-001")

        # Should complete but without that workflow's state
        assert "wf-001" not in transfer.workflow_states or \
               transfer.workflow_states.get("wf-001") is None


class TestInterruptedGracefulTransfer:
    """Tests for graceful transfers that get interrupted by failures."""

    @pytest.mark.asyncio
    async def test_interrupted_early_recovers_from_workers(self):
        """Early interruption requires full recovery from workers."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")

        workers = [coordinator.add_worker(f"worker-{i}") for i in range(5)]

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=[f"wf-{i}" for i in range(5)],
            worker_assignments={f"wf-{i}": f"worker-{i}" for i in range(5)},
        )

        # Set progress
        for i, w in enumerate(workers):
            w.active_workflows[f"wf-{i}"].completed_count = (i + 1) * 10

        # Interrupt at 10% (only 0 workflows transferred before crash)
        transfer = await coordinator.interrupted_graceful_transfer(
            "manager-a", "manager-b", "job-001",
            interrupt_point=0.1,
        )

        # All workflows should be recovered from workers
        assert len(transfer.workflow_states) == 5

    @pytest.mark.asyncio
    async def test_interrupted_late_has_partial_leader_state(self):
        """Late interruption has some state from leader transfer."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")

        workers = [coordinator.add_worker(f"worker-{i}") for i in range(5)]

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=[f"wf-{i}" for i in range(5)],
            worker_assignments={f"wf-{i}": f"worker-{i}" for i in range(5)},
        )

        for i, w in enumerate(workers):
            w.active_workflows[f"wf-{i}"].completed_count = (i + 1) * 10

        # Interrupt at 80% (4 workflows transferred)
        transfer = await coordinator.interrupted_graceful_transfer(
            "manager-a", "manager-b", "job-001",
            interrupt_point=0.8,
        )

        # All 5 workflows should be present (4 from transfer, 1 from recovery)
        assert len(transfer.workflow_states) == 5

    @pytest.mark.asyncio
    async def test_interrupted_clears_all_orphans(self):
        """Interrupted transfer still clears all orphan statuses."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")

        workers = [coordinator.add_worker(f"worker-{i}") for i in range(3)]

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-0", "wf-1", "wf-2"],
            worker_assignments={f"wf-{i}": f"worker-{i}" for i in range(3)},
        )

        await coordinator.interrupted_graceful_transfer(
            "manager-a", "manager-b", "job-001",
            interrupt_point=0.5,
        )

        # All workers should have orphan status cleared
        for w in workers:
            for wf_id in w.active_workflows:
                assert wf_id not in w.orphaned_workflows


class TestProgressPreservation:
    """Tests verifying workflow progress is preserved across transfer types."""

    @pytest.mark.asyncio
    async def test_progress_preserved_graceful(self):
        """Progress preserved through graceful transfer."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # Set specific progress
        workflow = worker.active_workflows["wf-001"]
        workflow.completed_count = 75
        workflow.checkpoint()

        original_progress = workflow.completed_count
        original_checkpoint = workflow.checkpointed_at_count

        await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")

        new_leader = coordinator.get_leader()
        transferred_workflow = new_leader.jobs["job-001"].workflows["wf-001"]

        assert transferred_workflow.completed_count == original_progress
        assert transferred_workflow.checkpointed_at_count == original_checkpoint

    @pytest.mark.asyncio
    async def test_progress_preserved_abrupt(self):
        """Progress preserved through abrupt transfer (from worker state)."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        workflow = worker.active_workflows["wf-001"]
        workflow.completed_count = 75

        await coordinator.abrupt_transfer("manager-a", "manager-b", "job-001")

        new_leader = coordinator.get_leader()
        transferred_workflow = new_leader.jobs["job-001"].workflows["wf-001"]

        assert transferred_workflow.completed_count == 75

    @pytest.mark.asyncio
    async def test_multiple_transfers_preserve_cumulative_progress(self):
        """Multiple transfers preserve cumulative progress."""
        coordinator = TransferCoordinator()

        manager_a = coordinator.add_manager("manager-a")
        manager_b = coordinator.add_manager("manager-b")
        manager_c = coordinator.add_manager("manager-c")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # Progress phase 1
        worker.active_workflows["wf-001"].completed_count = 25

        # Transfer A -> B
        await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")

        # More progress
        worker.active_workflows["wf-001"].completed_count = 50

        # Transfer B -> C
        await coordinator.graceful_transfer("manager-b", "manager-c", "job-001")

        # More progress
        worker.active_workflows["wf-001"].completed_count = 75

        # Verify final state
        assert manager_c.jobs["job-001"].workflows["wf-001"].completed_count == 75


class TestMixedTransferScenarios:
    """Tests for mixed graceful/abrupt transfer scenarios."""

    @pytest.mark.asyncio
    async def test_graceful_then_abrupt(self):
        """Graceful transfer followed by abrupt failure."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        coordinator.add_manager("manager-c")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        worker.active_workflows["wf-001"].completed_count = 30

        # Graceful: A -> B
        transfer1 = await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")
        assert transfer1.transfer_mode == TransferMode.GRACEFUL

        worker.active_workflows["wf-001"].completed_count = 60

        # Abrupt: B -> C
        transfer2 = await coordinator.abrupt_transfer("manager-b", "manager-c", "job-001")
        assert transfer2.transfer_mode == TransferMode.ABRUPT

        # Final progress preserved
        new_leader = coordinator.get_leader()
        assert new_leader.jobs["job-001"].workflows["wf-001"].completed_count == 60

    @pytest.mark.asyncio
    async def test_fence_tokens_always_increase(self):
        """Fence tokens increase regardless of transfer mode."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        coordinator.add_manager("manager-c")
        worker = coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        tokens = [worker.fence_tokens["job-001"]]

        # Graceful
        await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")
        tokens.append(worker.fence_tokens["job-001"])

        # Abrupt
        await coordinator.abrupt_transfer("manager-b", "manager-c", "job-001")
        tokens.append(worker.fence_tokens["job-001"])

        # Verify monotonic increase
        for i in range(1, len(tokens)):
            assert tokens[i] > tokens[i - 1]


class TestTransferHistory:
    """Tests for transfer history tracking."""

    @pytest.mark.asyncio
    async def test_history_records_all_transfers(self):
        """Transfer history captures all transfers."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        coordinator.add_manager("manager-c")
        coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")
        await coordinator.abrupt_transfer("manager-b", "manager-c", "job-001")

        assert len(coordinator._transfer_history) == 2
        assert coordinator._transfer_history[0].transfer_mode == TransferMode.GRACEFUL
        assert coordinator._transfer_history[1].transfer_mode == TransferMode.ABRUPT

    @pytest.mark.asyncio
    async def test_history_timestamps_ordered(self):
        """Transfer history has ordered timestamps."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        coordinator.add_manager("manager-c")
        coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")
        await asyncio.sleep(0.01)
        await coordinator.abrupt_transfer("manager-b", "manager-c", "job-001")

        t1 = coordinator._transfer_history[0].transfer_completed
        t2 = coordinator._transfer_history[1].transfer_completed

        assert t2 > t1


class TestEdgeCases:
    """Edge case tests."""

    @pytest.mark.asyncio
    async def test_transfer_single_workflow_job(self):
        """Single workflow job transfers correctly."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        transfer = await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")

        assert len(transfer.workflow_states) == 1

    @pytest.mark.asyncio
    async def test_transfer_large_job(self):
        """Large job with many workflows transfers correctly."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")

        num_workflows = 100
        workers = [coordinator.add_worker(f"worker-{i}") for i in range(10)]

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=[f"wf-{i:03d}" for i in range(num_workflows)],
            worker_assignments={f"wf-{i:03d}": f"worker-{i % 10}" for i in range(num_workflows)},
        )

        transfer = await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")

        assert len(transfer.workflow_states) == num_workflows

    @pytest.mark.asyncio
    async def test_transfer_back_to_original_leader(self):
        """Job can transfer back to original leader."""
        coordinator = TransferCoordinator()

        coordinator.add_manager("manager-a")
        coordinator.add_manager("manager-b")
        coordinator.add_worker("worker-1")

        coordinator.elect_leader("manager-a")
        coordinator.submit_job(
            job_id="job-001",
            workflow_ids=["wf-001"],
            worker_assignments={"wf-001": "worker-1"},
        )

        # A -> B
        await coordinator.graceful_transfer("manager-a", "manager-b", "job-001")

        # Re-add job to A for transfer back
        coordinator.managers["manager-a"].jobs["job-001"] = coordinator.managers["manager-b"].jobs["job-001"]
        coordinator.managers["manager-a"].fence_tokens["job-001"] = 2

        # B -> A
        await coordinator.graceful_transfer("manager-b", "manager-a", "job-001")

        assert coordinator.get_leader().manager_id == "manager-a"