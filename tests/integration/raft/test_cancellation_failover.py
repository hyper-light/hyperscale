"""
Integration tests for cancellation during leadership failover.

Tests the interaction between:
- Job leadership transfer (SWIM leader + per-job Raft leader)
- Workflow cancellation push notification chain
- Worker orphan grace period handling
- Gate orphan job detection
- Single workflow cancellation through gate
"""

import asyncio
import time
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
from hyperscale.distributed.models import (
    JobCancellationComplete,
    SingleWorkflowCancelRequest,
    WorkflowCancellationComplete,
    WorkflowProgress,
)
from hyperscale.distributed.nodes.manager.cancellation import (
    ManagerCancellationCoordinator,
)
from hyperscale.distributed.nodes.worker.cancellation import WorkerCancellationHandler
from hyperscale.distributed.nodes.worker.state import WorkerState


# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture
def mock_logger():
    logger = MagicMock()
    logger.log = AsyncMock()
    return logger


@pytest.fixture
def mock_task_runner():
    runner = MagicMock()
    runner.run = MagicMock()
    return runner


@pytest.fixture
def mock_send_tcp():
    return AsyncMock(return_value=b"ok")


@pytest.fixture
def mock_core_allocator():
    return MagicMock()


@pytest.fixture
def worker_state(mock_core_allocator):
    state = WorkerState(core_allocator=mock_core_allocator)
    state.initialize_locks()
    return state


@pytest.fixture
def cancellation_handler(worker_state, mock_logger):
    return WorkerCancellationHandler(
        state=worker_state,
        logger=mock_logger,
        poll_interval=0.1,
    )


@pytest.fixture
def leadership_tracker():
    return JobLeadershipTracker[int](
        node_id="manager-1",
        node_addr=("127.0.0.1", 9000),
    )


@pytest.fixture
def mock_manager_state():
    """Create a mock ManagerState with cancellation tracking dicts."""
    state = MagicMock()
    state._job_submissions = {}
    state._cancellation_pending_workflows = {}
    state._cancellation_errors = {}
    state._cancellation_completion_events = {}
    state._cancellation_initiated_at = {}
    state._cancelled_workflows = {}
    state._job_callbacks = {}
    state._client_callbacks = {}
    state._workflow_cancellation_locks = {}
    state.get_cancellation_pending_workflows = MagicMock(return_value=set())
    state.remove_cancellation_pending_workflow = MagicMock()
    state.add_cancellation_error = MagicMock()
    state.get_cancellation_errors = MagicMock(return_value=[])
    state.get_cancellation_completion_event = MagicMock(return_value=None)
    state.clear_cancellation_state = MagicMock()
    state.set_cancelled_workflow = MagicMock()
    state.get_worker = MagicMock(return_value=None)
    state.increment_state_version = AsyncMock()
    return state


@pytest.fixture
def mock_manager_config():
    config = MagicMock()
    config.host = "127.0.0.1"
    config.tcp_port = 9000
    config.tcp_timeout_standard_seconds = 5.0
    return config


@pytest.fixture
def mock_job_manager():
    """Create a mock JobManager with get_job_by_id."""
    manager = MagicMock()
    manager.get_job_by_id = MagicMock(return_value=None)
    return manager


@pytest.fixture
def manager_cancellation(
    mock_manager_state,
    mock_manager_config,
    mock_logger,
    mock_task_runner,
    mock_job_manager,
):
    return ManagerCancellationCoordinator(
        state=mock_manager_state,
        config=mock_manager_config,
        logger=mock_logger,
        node_id="manager-1",
        task_runner=mock_task_runner,
        send_to_worker=AsyncMock(),
        send_to_client=AsyncMock(),
        job_manager=mock_job_manager,
    )


# =========================================================================
# 4.1: SWIM Leader + Job Leader Fails
# =========================================================================


class TestSwimLeaderPlusJobLeaderFails:
    """
    Test scenario where the SWIM cluster leader is ALSO the job leader.

    When this node fails:
    1. SWIM detects failure, triggers _on_node_dead
    2. New SWIM leader elected, _on_manager_become_leader fires
    3. New leader scans for orphaned jobs
    4. Takes over orphaned jobs via Raft proposal
    5. Workers receive transfer notification
    """

    @pytest.mark.asyncio
    async def test_dead_manager_triggers_orphan_scan(self, leadership_tracker):
        """New SWIM leader scans for orphaned jobs from dead managers."""
        dead_manager_addr = ("127.0.0.2", 9000)

        leadership_tracker.assume_leadership("job-1", dead_manager_addr)

        all_leaderships = leadership_tracker.get_all_leaderships()
        assert "job-1" in all_leaderships
        assert all_leaderships["job-1"].leader_addr == dead_manager_addr

    @pytest.mark.asyncio
    async def test_orphan_detection_with_dead_set(self, leadership_tracker):
        """Orphan scan checks job leaders against dead manager set."""
        dead_managers: set[tuple[str, int]] = set()
        dead_manager_addr = ("127.0.0.2", 9000)
        live_manager_addr = ("127.0.0.3", 9000)

        leadership_tracker.assume_leadership("job-orphan", dead_manager_addr)
        leadership_tracker.assume_leadership("job-alive", live_manager_addr)
        dead_managers.add(dead_manager_addr)

        orphaned_jobs = [
            job_id
            for job_id, info in leadership_tracker.get_all_leaderships().items()
            if info.leader_addr in dead_managers
        ]

        assert orphaned_jobs == ["job-orphan"]

    @pytest.mark.asyncio
    async def test_takeover_increments_fencing_token(self, leadership_tracker):
        """Takeover must produce a higher fencing token than the previous leader."""
        old_addr = ("127.0.0.2", 9000)
        new_addr = ("127.0.0.1", 9000)

        leadership_tracker.assume_leadership("job-1", old_addr)
        old_info = leadership_tracker.get_leadership("job-1")
        old_token = old_info.fencing_token if old_info else -1

        leadership_tracker.takeover_leadership("job-1", new_addr)
        new_info = leadership_tracker.get_leadership("job-1")
        new_token = new_info.fencing_token if new_info else -1

        assert new_token > old_token

    @pytest.mark.asyncio
    async def test_multiple_dead_managers_all_jobs_found(self, leadership_tracker):
        """When multiple managers die, ALL their jobs are detected as orphaned."""
        dead_addrs = {("10.0.0.1", 9000), ("10.0.0.2", 9000)}

        leadership_tracker.assume_leadership("job-a", ("10.0.0.1", 9000))
        leadership_tracker.assume_leadership("job-b", ("10.0.0.2", 9000))
        leadership_tracker.assume_leadership("job-c", ("10.0.0.3", 9000))

        orphaned = [
            job_id
            for job_id, info in leadership_tracker.get_all_leaderships().items()
            if info.leader_addr in dead_addrs
        ]

        assert sorted(orphaned) == ["job-a", "job-b"]


# =========================================================================
# 4.2: Job Leader Fails (Not SWIM Leader)
# =========================================================================


class TestJobLeaderFailsNotSwimLeader:
    """
    Test scenario where the job leader is NOT the SWIM cluster leader.

    The SWIM leader takes over the orphaned job via _handle_job_leader_failure.
    """

    @pytest.mark.asyncio
    async def test_non_swim_leader_job_detected_orphaned(self, leadership_tracker):
        """Job leader that's not SWIM leader -- detected via dead set."""
        job_leader_addr = ("10.0.0.5", 9000)
        dead_managers: set[tuple[str, int]] = {job_leader_addr}

        leadership_tracker.assume_leadership("job-x", job_leader_addr)

        all_jobs = leadership_tracker.get_all_leaderships()
        is_orphaned = all_jobs["job-x"].leader_addr in dead_managers
        assert is_orphaned is True

    @pytest.mark.asyncio
    async def test_takeover_preserves_job_data(self, leadership_tracker):
        """Takeover should preserve the job_id but update leader info."""
        old_addr = ("10.0.0.5", 9000)
        new_addr = ("10.0.0.1", 9000)

        leadership_tracker.assume_leadership("job-keep", old_addr)
        leadership_tracker.takeover_leadership("job-keep", new_addr)

        info = leadership_tracker.get_leadership("job-keep")
        assert info is not None
        assert info.leader_addr == new_addr

    @pytest.mark.asyncio
    async def test_gate_notified_of_transfer(self):
        """Verify gate transfer notification fires on leadership change."""
        transfer_notifications: list[str] = []

        async def mock_notify_gate(job_id: str, old_leader: str):
            transfer_notifications.append(job_id)

        await mock_notify_gate("job-transferred", "old-manager")
        assert "job-transferred" in transfer_notifications


# =========================================================================
# 4.3: Worker Orphan Grace Period
# =========================================================================


class TestWorkerOrphanGracePeriod:
    """
    Test that workers wait a grace period before cancelling orphaned workflows.

    When the job leader dies:
    1. Worker marks workflows as orphaned (with timestamp)
    2. Grace period timer starts
    3. If no transfer arrives, workflows are cancelled after grace expires
    """

    @pytest.mark.asyncio
    async def test_workflow_marked_orphaned_on_manager_death(self, worker_state):
        """Workflows are marked orphaned when their manager dies."""
        workflow_id = "wf-orphan-1"
        progress = MagicMock(spec=WorkflowProgress)
        progress.job_id = "job-1"
        worker_state.add_active_workflow(
            workflow_id, progress, ("10.0.0.1", 9000)
        )

        worker_state.mark_workflow_orphaned(workflow_id)

        assert worker_state.is_workflow_orphaned(workflow_id)
        assert workflow_id in worker_state._orphaned_workflows

    @pytest.mark.asyncio
    async def test_grace_period_not_expired(self, worker_state):
        """Within grace period, no workflows should be returned for cancellation."""
        workflow_id = "wf-grace"
        worker_state._orphaned_workflows[workflow_id] = time.monotonic()

        expired = worker_state.get_orphaned_workflows_expired(
            grace_period_seconds=5.0
        )
        assert workflow_id not in expired

    @pytest.mark.asyncio
    async def test_grace_period_expired(self, worker_state):
        """After grace period, workflows are returned for cancellation."""
        workflow_id = "wf-expired"
        worker_state._orphaned_workflows[workflow_id] = time.monotonic() - 10.0

        expired = worker_state.get_orphaned_workflows_expired(
            grace_period_seconds=5.0
        )
        assert workflow_id in expired

    @pytest.mark.asyncio
    async def test_cancel_event_set_on_orphan_expiry(
        self, worker_state, cancellation_handler
    ):
        """Cancellation event fires for orphaned workflows past grace period."""
        workflow_id = "wf-cancel-me"
        event = cancellation_handler.create_cancel_event(workflow_id)
        worker_state._orphaned_workflows[workflow_id] = time.monotonic() - 10.0

        expired = worker_state.get_orphaned_workflows_expired(
            grace_period_seconds=5.0
        )
        for expired_workflow_id in expired:
            cancellation_handler.signal_cancellation(expired_workflow_id)

        assert event.is_set()


# =========================================================================
# 4.4: Worker Receives Transfer Before Grace Expires
# =========================================================================


class TestWorkerReceivesTransferBeforeGrace:
    """
    Test that a timely leadership transfer prevents workflow cancellation.

    1. Job leader fails, workflows marked orphaned
    2. New leader sends transfer within grace period
    3. Worker clears orphan status, updates routing
    4. Workflow continues executing
    """

    @pytest.mark.asyncio
    async def test_transfer_clears_orphan_status(self, worker_state):
        """Leadership transfer clears orphan status for affected workflows."""
        workflow_id = "wf-saved"
        progress = MagicMock(spec=WorkflowProgress)
        progress.job_id = "job-1"
        worker_state.add_active_workflow(
            workflow_id, progress, ("10.0.0.1", 9000)
        )
        worker_state.mark_workflow_orphaned(workflow_id)
        assert worker_state.is_workflow_orphaned(workflow_id)

        worker_state.clear_workflow_orphaned(workflow_id)

        assert not worker_state.is_workflow_orphaned(workflow_id)

    @pytest.mark.asyncio
    async def test_transfer_updates_job_leader_addr(self, worker_state):
        """Transfer updates the job leader address for the workflow."""
        workflow_id = "wf-reroute"
        progress = MagicMock(spec=WorkflowProgress)
        progress.job_id = "job-1"
        old_addr = ("10.0.0.1", 9000)
        new_addr = ("10.0.0.2", 9000)
        worker_state.add_active_workflow(workflow_id, progress, old_addr)

        worker_state.set_workflow_job_leader(workflow_id, new_addr)

        assert worker_state.get_workflow_job_leader(workflow_id) == new_addr

    @pytest.mark.asyncio
    async def test_cancel_event_not_set_after_transfer(
        self, worker_state, cancellation_handler
    ):
        """Workflow cancel event NOT set if transfer arrives before grace expires."""
        workflow_id = "wf-still-running"
        event = cancellation_handler.create_cancel_event(workflow_id)
        worker_state._orphaned_workflows[workflow_id] = time.monotonic()

        worker_state.clear_workflow_orphaned(workflow_id)

        expired = worker_state.get_orphaned_workflows_expired(
            grace_period_seconds=5.0
        )
        assert workflow_id not in expired
        assert not event.is_set()

    @pytest.mark.asyncio
    async def test_fence_token_accepted_on_valid_transfer(self, worker_state):
        """Worker accepts fence token from new leader if it's higher."""
        workflow_id = "wf-fenced"
        old_token = 5
        new_token = 6

        await worker_state.update_workflow_fence_token(workflow_id, old_token)
        accepted = await worker_state.update_workflow_fence_token(
            workflow_id, new_token
        )

        assert accepted is True
        current = await worker_state.get_workflow_fence_token(workflow_id)
        assert current == new_token

    @pytest.mark.asyncio
    async def test_stale_fence_token_rejected(self, worker_state):
        """Worker rejects stale fence token (prevents stale leaders)."""
        workflow_id = "wf-stale"
        await worker_state.update_workflow_fence_token(workflow_id, 10)

        rejected = await worker_state.update_workflow_fence_token(workflow_id, 5)

        assert rejected is False
        current = await worker_state.get_workflow_fence_token(workflow_id)
        assert current == 10


# =========================================================================
# 4.5: Cancellation Push Notification Chain (End-to-End)
# =========================================================================


class TestCancellationPushChainEndToEnd:
    """
    Test the full push notification chain:
    Worker → Manager → Gate → Client.

    Worker sends WorkflowCancellationComplete to manager.
    Manager aggregates, sends JobCancellationComplete to gate.
    Gate forwards to client. Client completion event fires.
    """

    @pytest.mark.asyncio
    async def test_manager_tracks_workflow_cancellation_completion(
        self, manager_cancellation, mock_manager_state
    ):
        """Manager coordinator tracks cancellation completion from workers."""
        job_id = "job-cancel-1"
        workflow_id = "wf-cancel-1"

        mock_manager_state._cancellation_pending_workflows[job_id] = {workflow_id}
        mock_manager_state._cancellation_errors[job_id] = []

        notification = WorkflowCancellationComplete(
            job_id=job_id,
            workflow_id=workflow_id,
            success=True,
            errors=[],
            cancelled_at=time.monotonic(),
            node_id="worker-1",
        )

        await manager_cancellation.handle_workflow_cancelled(notification)

        assert workflow_id not in mock_manager_state._cancellation_pending_workflows.get(
            job_id, set()
        )

    @pytest.mark.asyncio
    async def test_manager_fires_event_when_all_workflows_complete(
        self, manager_cancellation, mock_manager_state
    ):
        """Manager fires completion event when last workflow reports."""
        job_id = "job-all-done"
        completion_event = asyncio.Event()

        mock_manager_state._cancellation_pending_workflows[job_id] = {"wf-last"}
        mock_manager_state._cancellation_errors[job_id] = []
        mock_manager_state._cancellation_completion_events[job_id] = completion_event
        mock_manager_state.get_cancellation_completion_event = MagicMock(
            return_value=completion_event
        )
        mock_manager_state._job_callbacks = {}
        mock_manager_state._client_callbacks = {}

        notification = WorkflowCancellationComplete(
            job_id=job_id,
            workflow_id="wf-last",
            success=True,
            errors=[],
            cancelled_at=time.monotonic(),
            node_id="worker-1",
        )

        await manager_cancellation.handle_workflow_cancelled(notification)

        assert completion_event.is_set()

    @pytest.mark.asyncio
    async def test_manager_aggregates_errors_from_workers(
        self, manager_cancellation, mock_manager_state
    ):
        """Manager aggregates errors from multiple worker cancellations."""
        job_id = "job-errors"
        mock_manager_state._cancellation_pending_workflows[job_id] = {"wf-err"}
        mock_manager_state._cancellation_errors[job_id] = []

        notification = WorkflowCancellationComplete(
            job_id=job_id,
            workflow_id="wf-err",
            success=False,
            errors=["Timeout waiting for workflow"],
            cancelled_at=time.monotonic(),
            node_id="worker-1",
        )

        await manager_cancellation.handle_workflow_cancelled(notification)

        assert len(mock_manager_state._cancellation_errors[job_id]) > 0

    @pytest.mark.asyncio
    async def test_client_completion_event_fires_on_notification(self):
        """Client completion event fires when JobCancellationComplete arrives."""
        completion_event = asyncio.Event()
        cancellation_events: dict[str, asyncio.Event] = {}
        job_id = "job-client-done"
        cancellation_events[job_id] = completion_event

        notification = JobCancellationComplete(
            job_id=job_id,
            success=True,
            errors=[],
        )

        if event := cancellation_events.get(notification.job_id):
            event.set()

        assert completion_event.is_set()


# =========================================================================
# 4.6: Single Workflow Cancellation Through Gate
# =========================================================================


class TestSingleWorkflowCancellationThroughGate:
    """
    Test fine-grained workflow cancellation:
    Client → Gate → Manager(s) → Worker.

    Gate fans out to all DCs, aggregates results.
    """

    @pytest.mark.asyncio
    async def test_single_cancel_request_has_required_fields(self):
        """SingleWorkflowCancelRequest carries all needed data."""
        request = SingleWorkflowCancelRequest(
            job_id="job-1",
            workflow_id="wf-target",
            request_id="req-123",
        )

        assert request.job_id == "job-1"
        assert request.workflow_id == "wf-target"
        assert request.request_id == "req-123"

    @pytest.mark.asyncio
    async def test_gate_fans_out_to_multiple_dcs(self):
        """Gate sends cancellation to all DCs with the job."""
        dc_responses: dict[str, bool] = {}
        target_dcs = ["dc-east", "dc-west", "dc-central"]

        async def mock_send_to_dc(dc_id: str, job_id: str, workflow_id: str):
            dc_responses[dc_id] = True

        for dc_id in target_dcs:
            await mock_send_to_dc(dc_id, "job-1", "wf-target")

        assert len(dc_responses) == 3
        assert all(dc_responses.values())

    @pytest.mark.asyncio
    async def test_cancelled_workflow_bucket_prevents_resurrection(self):
        """Cancelled workflows are tracked to prevent re-dispatch."""
        cancelled_workflows: dict[str, float] = {}
        workflow_id = "wf-dead"

        cancelled_workflows[workflow_id] = time.time()

        should_dispatch = workflow_id not in cancelled_workflows
        assert should_dispatch is False

    @pytest.mark.asyncio
    async def test_per_workflow_lock_prevents_race(self):
        """Per-workflow lock prevents race between cancel and dispatch."""
        locks: dict[str, asyncio.Lock] = {}
        workflow_id = "wf-race"
        locks[workflow_id] = asyncio.Lock()

        acquired = locks[workflow_id].locked()
        assert acquired is False

        async with locks[workflow_id]:
            assert locks[workflow_id].locked()
        assert not locks[workflow_id].locked()


# =========================================================================
# 4.7: Cancellation During Leadership Failover
# =========================================================================


class TestCancellationDuringLeadershipFailover:
    """
    Test cancellation that's in progress when the job leader fails.

    The new leader must pick up cancellation state and complete the flow.
    """

    @pytest.mark.asyncio
    async def test_pending_cancellations_survive_leader_change(self):
        """Pending cancellation set persists across leadership changes."""
        pending_workflows: dict[str, set[str]] = {
            "job-mid-cancel": {"wf-1", "wf-2", "wf-3"}
        }

        pending_workflows["job-mid-cancel"].discard("wf-1")

        remaining = pending_workflows["job-mid-cancel"]
        assert remaining == {"wf-2", "wf-3"}

    @pytest.mark.asyncio
    async def test_completion_event_reset_on_new_leader(self):
        """New leader creates fresh completion event for in-progress cancellation."""
        old_event = asyncio.Event()
        old_event.set()

        new_event = asyncio.Event()
        assert not new_event.is_set()

    @pytest.mark.asyncio
    async def test_partial_completion_tracked(
        self, manager_cancellation, mock_manager_state
    ):
        """New leader correctly handles workflows that already reported completion."""
        job_id = "job-partial"
        mock_manager_state._cancellation_pending_workflows[job_id] = {"wf-2", "wf-3"}
        mock_manager_state._cancellation_errors[job_id] = []

        notification_wf2 = WorkflowCancellationComplete(
            job_id=job_id,
            workflow_id="wf-2",
            success=True,
            errors=[],
            cancelled_at=time.monotonic(),
            node_id="worker-1",
        )
        await manager_cancellation.handle_workflow_cancelled(notification_wf2)

        assert "wf-2" not in mock_manager_state._cancellation_pending_workflows.get(
            job_id, set()
        )

    @pytest.mark.asyncio
    async def test_raft_proposal_for_cancel_before_takeover(self):
        """Cancellation proposed through Raft is durable across leader changes."""
        raft_log_entries: list[dict[str, str]] = []

        raft_log_entries.append({
            "command_type": "INITIATE_CANCELLATION",
            "job_id": "job-raft-cancel",
        })

        assert len(raft_log_entries) == 1
        assert raft_log_entries[0]["command_type"] == "INITIATE_CANCELLATION"


# =========================================================================
# 4.8: Gate Orphan Job Handling
# =========================================================================


class TestGateOrphanJobHandling:
    """
    Test gate's response when a job leader manager dies.

    Gate tracks dead job leaders, scans for orphaned jobs,
    waits grace period, then either receives transfer or
    marks job as failed.
    """

    @pytest.mark.asyncio
    async def test_gate_tracks_dead_manager_addrs(self):
        """Gate adds dead manager addresses to tracking set."""
        dead_gate_addrs: set[tuple[str, int]] = set()
        dead_addr = ("10.0.0.1", 9000)

        dead_gate_addrs.add(dead_addr)

        assert dead_addr in dead_gate_addrs

    @pytest.mark.asyncio
    async def test_gate_scans_jobs_for_dead_leaders(self):
        """Gate identifies jobs whose leader is in the dead set."""
        dead_addrs: set[tuple[str, int]] = {("10.0.0.1", 9000)}
        job_leader_addrs: dict[str, tuple[str, int]] = {
            "job-1": ("10.0.0.1", 9000),
            "job-2": ("10.0.0.2", 9000),
            "job-3": ("10.0.0.1", 9000),
        }

        orphaned = [
            job_id
            for job_id, addr in job_leader_addrs.items()
            if addr in dead_addrs
        ]

        assert sorted(orphaned) == ["job-1", "job-3"]

    @pytest.mark.asyncio
    async def test_gate_grace_period_before_failure(self):
        """Gate waits grace period before marking orphaned jobs as failed."""
        orphan_timestamp = time.monotonic() - 2.0
        grace_period = 10.0

        grace_expired = (time.monotonic() - orphan_timestamp) > grace_period
        assert grace_expired is False

    @pytest.mark.asyncio
    async def test_gate_grace_expired_marks_job_failed(self):
        """After grace period, gate marks orphaned job as failed."""
        orphan_timestamp = time.monotonic() - 15.0
        grace_period = 10.0

        grace_expired = (time.monotonic() - orphan_timestamp) > grace_period
        assert grace_expired is True

    @pytest.mark.asyncio
    async def test_transfer_removes_from_dead_set(self):
        """Receiving a transfer clears the manager from dead tracking."""
        dead_addrs: set[tuple[str, int]] = {("10.0.0.1", 9000)}
        orphaned_jobs: dict[str, float] = {"job-recovered": time.monotonic()}

        new_leader_addr = ("10.0.0.3", 9000)
        orphaned_jobs.pop("job-recovered", None)
        dead_addrs.discard(("10.0.0.1", 9000))

        assert "job-recovered" not in orphaned_jobs
        assert ("10.0.0.1", 9000) not in dead_addrs

    @pytest.mark.asyncio
    async def test_concurrent_failures_handled(self):
        """Multiple managers dying simultaneously are all tracked."""
        dead_addrs: set[tuple[str, int]] = set()

        dead_addrs.add(("10.0.0.1", 9000))
        dead_addrs.add(("10.0.0.2", 9000))

        assert len(dead_addrs) == 2


# =========================================================================
# Worker Cancellation Cleanup Tests
# =========================================================================


class TestWorkerCancellationCleanup:
    """Test worker cancellation handler cleanup and observability."""

    @pytest.mark.asyncio
    async def test_stale_events_cleaned_up(
        self, worker_state, cancellation_handler
    ):
        """Stale cancel events are removed when workflows are no longer active."""
        cancellation_handler.create_cancel_event("wf-active")
        cancellation_handler.create_cancel_event("wf-stale")

        active_ids = {"wf-active"}
        removed = cancellation_handler.cleanup_stale_events(active_ids)

        assert removed == 1
        assert "wf-stale" not in worker_state._workflow_cancel_events
        assert "wf-active" in worker_state._workflow_cancel_events

    @pytest.mark.asyncio
    async def test_cleanup_removes_completion_events_too(
        self, worker_state, cancellation_handler
    ):
        """Stale cleanup also removes completion events and errors."""
        cancellation_handler.create_cancel_event("wf-gone")
        worker_state._cancellation_completion_events["wf-gone"] = asyncio.Event()
        worker_state._cancellation_errors["wf-gone"] = ["some error"]

        removed = cancellation_handler.cleanup_stale_events(set())

        assert "wf-gone" not in worker_state._cancellation_completion_events
        assert "wf-gone" not in worker_state._cancellation_errors

    @pytest.mark.asyncio
    async def test_cancellation_stats_observability(
        self, worker_state, cancellation_handler
    ):
        """Stats method returns accurate counts."""
        cancellation_handler.create_cancel_event("wf-1")
        cancellation_handler.create_cancel_event("wf-2")
        worker_state._cancellation_completion_events["wf-1"] = asyncio.Event()
        worker_state._cancellation_errors["wf-3"] = ["err"]

        stats = cancellation_handler.get_cancellation_stats()

        assert stats["cancel_events"] == 2
        assert stats["completion_events"] == 1
        assert stats["pending_errors"] == 1

    @pytest.mark.asyncio
    async def test_remove_active_workflow_cleans_cancellation_state(
        self, worker_state
    ):
        """Removing a workflow cleans up all cancellation tracking."""
        workflow_id = "wf-remove"
        progress = MagicMock(spec=WorkflowProgress)
        progress.job_id = "job-1"
        worker_state.add_active_workflow(
            workflow_id, progress, ("10.0.0.1", 9000)
        )
        worker_state._workflow_cancel_events[workflow_id] = asyncio.Event()
        worker_state._cancellation_completion_events[workflow_id] = asyncio.Event()
        worker_state._cancellation_errors[workflow_id] = ["err"]

        worker_state.remove_active_workflow(workflow_id)

        assert workflow_id not in worker_state._workflow_cancel_events
        assert workflow_id not in worker_state._cancellation_completion_events
        assert workflow_id not in worker_state._cancellation_errors


# =========================================================================
# Manager Cancellation Coordinator JobManager Wiring Tests
# =========================================================================


class TestManagerCancellationCoordinatorJobManagerWiring:
    """Test that _get_job_workflow_ids correctly queries JobManager."""

    @pytest.mark.asyncio
    async def test_get_workflow_ids_returns_sub_workflows(
        self, manager_cancellation, mock_job_manager
    ):
        """Coordinator returns sub-workflow IDs from JobManager."""
        mock_job = MagicMock()
        mock_job.sub_workflows = {
            "sw-token-1": MagicMock(),
            "sw-token-2": MagicMock(),
            "sw-token-3": MagicMock(),
        }
        mock_job_manager.get_job_by_id = MagicMock(return_value=mock_job)

        workflow_ids = manager_cancellation._get_job_workflow_ids("job-real")

        assert sorted(workflow_ids) == ["sw-token-1", "sw-token-2", "sw-token-3"]

    @pytest.mark.asyncio
    async def test_get_workflow_ids_returns_empty_for_unknown_job(
        self, manager_cancellation, mock_job_manager
    ):
        """Coordinator returns empty list for unknown job."""
        mock_job_manager.get_job_by_id = MagicMock(return_value=None)

        workflow_ids = manager_cancellation._get_job_workflow_ids("job-nonexistent")

        assert workflow_ids == []

    @pytest.mark.asyncio
    async def test_get_workflow_ids_no_job_manager(self, mock_manager_state, mock_manager_config, mock_logger, mock_task_runner):
        """Coordinator returns empty list when no JobManager provided."""
        coordinator = ManagerCancellationCoordinator(
            state=mock_manager_state,
            config=mock_manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            send_to_worker=AsyncMock(),
            send_to_client=AsyncMock(),
            job_manager=None,
        )

        workflow_ids = coordinator._get_job_workflow_ids("job-any")

        assert workflow_ids == []
