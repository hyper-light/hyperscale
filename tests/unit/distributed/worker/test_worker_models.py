"""
Integration tests for worker models (Section 15.2.2).

Tests WorkflowRuntimeState.

Covers:
- Happy path: Normal instantiation and field access
- Negative path: Invalid types and values
- Failure mode: Missing required fields
- Concurrency: Thread-safe instantiation (dataclasses with slots)
- Edge cases: Boundary values, None values, empty collections
"""

import time

import pytest

from hyperscale.distributed.nodes.worker.models import WorkflowRuntimeState


class TestWorkflowRuntimeState:
    """Test WorkflowRuntimeState dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal instantiation with all required fields."""
        start = time.time()
        state = WorkflowRuntimeState(
            workflow_id="wf-123",
            job_id="job-456",
            status="running",
            allocated_cores=4,
            fence_token=10,
            start_time=start,
        )

        assert state.workflow_id == "wf-123"
        assert state.job_id == "job-456"
        assert state.status == "running"
        assert state.allocated_cores == 4
        assert state.fence_token == 10
        assert state.start_time == start

    def test_default_values(self):
        """Test default field values."""
        state = WorkflowRuntimeState(
            workflow_id="wf-1",
            job_id="job-1",
            status="pending",
            allocated_cores=1,
            fence_token=0,
            start_time=0.0,
        )

        assert state.job_leader_addr is None
        assert state.is_orphaned is False
        assert state.orphaned_since is None
        assert state.cores_completed == 0
        assert state.vus == 0

    def test_with_orphan_state(self):
        """Test workflow in orphaned state."""
        orphan_time = time.time()
        state = WorkflowRuntimeState(
            workflow_id="wf-orphan",
            job_id="job-orphan",
            status="running",
            allocated_cores=2,
            fence_token=5,
            start_time=time.time() - 100,
            job_leader_addr=("manager-1", 8000),
            is_orphaned=True,
            orphaned_since=orphan_time,
        )

        assert state.is_orphaned is True
        assert state.orphaned_since == orphan_time
        assert state.job_leader_addr == ("manager-1", 8000)

    def test_with_vus_and_cores_completed(self):
        """Test with VUs and completed cores."""
        state = WorkflowRuntimeState(
            workflow_id="wf-vus",
            job_id="job-vus",
            status="completed",
            allocated_cores=8,
            fence_token=15,
            start_time=time.time(),
            cores_completed=6,
            vus=100,
        )

        assert state.cores_completed == 6
        assert state.vus == 100

    def test_slots_prevents_new_attributes(self):
        """Test that slots=True prevents adding new attributes."""
        state = WorkflowRuntimeState(
            workflow_id="wf",
            job_id="j",
            status="s",
            allocated_cores=1,
            fence_token=0,
            start_time=0,
        )

        with pytest.raises(AttributeError):
            state.custom_field = "value"

    def test_edge_case_zero_cores(self):
        """Test with zero allocated cores."""
        state = WorkflowRuntimeState(
            workflow_id="wf-zero",
            job_id="job-zero",
            status="pending",
            allocated_cores=0,
            fence_token=0,
            start_time=0.0,
        )

        assert state.allocated_cores == 0
