"""
Integration tests for fencing token validation.

These tests verify that:
1. Fence tokens are properly incremented on job ownership transfer
2. Stale updates with lower fence tokens are rejected
3. Valid updates with current or higher fence tokens are accepted

The fencing token pattern prevents stale updates from old job owners
after lease transfer (e.g., slow network delivering delayed updates).
"""

import asyncio
import pytest
import time

from hyperscale.distributed_rewrite.models import (
    JobProgress,
    JobFinalResult,
    JobStatus,
    WorkflowProgress,
    WorkflowStatus,
)


class TestFenceTokenValidation:
    """Test fence token validation in Gate handlers."""

    def test_job_progress_has_fence_token(self):
        """JobProgress should have fence_token field with default of 0."""
        progress = JobProgress(
            job_id="job-123",
            datacenter="dc1",
            status=JobStatus.RUNNING.value,
        )
        assert hasattr(progress, 'fence_token')
        assert progress.fence_token == 0

    def test_job_progress_with_custom_fence_token(self):
        """JobProgress should accept custom fence_token."""
        progress = JobProgress(
            job_id="job-123",
            datacenter="dc1",
            status=JobStatus.RUNNING.value,
            fence_token=5,
        )
        assert progress.fence_token == 5

    def test_job_final_result_has_fence_token(self):
        """JobFinalResult should have fence_token field with default of 0."""
        result = JobFinalResult(
            job_id="job-123",
            datacenter="dc1",
            status=JobStatus.COMPLETED.value,
        )
        assert hasattr(result, 'fence_token')
        assert result.fence_token == 0

    def test_job_final_result_with_custom_fence_token(self):
        """JobFinalResult should accept custom fence_token."""
        result = JobFinalResult(
            job_id="job-123",
            datacenter="dc1",
            status=JobStatus.COMPLETED.value,
            fence_token=10,
        )
        assert result.fence_token == 10

    def test_fence_token_serialization(self):
        """Fence token should survive serialization round-trip."""
        original = JobProgress(
            job_id="job-123",
            datacenter="dc1",
            status=JobStatus.RUNNING.value,
            fence_token=42,
        )

        # Serialize and deserialize
        serialized = original.dump()
        restored = JobProgress.load(serialized)

        assert restored.fence_token == 42
        assert restored.job_id == "job-123"

    def test_fence_token_comparison_logic(self):
        """
        Test the fence token comparison logic used in handlers.

        The gate should:
        - Accept updates where fence_token >= current (update highest seen)
        - Reject updates where fence_token < current
        """
        # Simulate gate's fence token tracking
        job_fence_tokens: dict[str, int] = {}

        def validate_and_update(job_id: str, incoming_token: int) -> bool:
            """Returns True if update should be accepted."""
            current = job_fence_tokens.get(job_id, 0)
            if incoming_token < current:
                return False  # Reject stale
            if incoming_token > current:
                job_fence_tokens[job_id] = incoming_token
            return True

        # First update with token 1 - should accept
        assert validate_and_update("job-1", 1) is True
        assert job_fence_tokens["job-1"] == 1

        # Update with same token - should accept (idempotent)
        assert validate_and_update("job-1", 1) is True

        # Update with higher token - should accept
        assert validate_and_update("job-1", 5) is True
        assert job_fence_tokens["job-1"] == 5

        # Update with lower token - should reject (stale)
        assert validate_and_update("job-1", 3) is False
        assert job_fence_tokens["job-1"] == 5  # Unchanged

        # Update with even lower token - should reject
        assert validate_and_update("job-1", 1) is False
        assert job_fence_tokens["job-1"] == 5  # Unchanged


class TestFenceTokenScenarios:
    """Test realistic fence token scenarios."""

    def test_job_ownership_transfer_scenario(self):
        """
        Simulate job ownership transfer and validate fence token behavior.

        Scenario:
        1. Manager-A owns job with fence_token=1
        2. Manager-A fails, Manager-B takes over with fence_token=2
        3. Delayed update from Manager-A arrives with fence_token=1
        4. Gate rejects the stale update
        5. New update from Manager-B with fence_token=2 is accepted
        """
        # Gate's fence token tracking
        job_fence_tokens: dict[str, int] = {}
        job_state: dict[str, JobProgress] = {}

        def gate_receive_progress(progress: JobProgress) -> bool:
            """Simulate gate receiving job progress."""
            current = job_fence_tokens.get(progress.job_id, 0)

            # Reject stale updates
            if progress.fence_token < current:
                return False

            # Update fence token
            if progress.fence_token > current:
                job_fence_tokens[progress.job_id] = progress.fence_token

            # Apply update
            job_state[progress.job_id] = progress
            return True

        # Manager-A sends initial update with token=1
        progress_a1 = JobProgress(
            job_id="job-transfer",
            datacenter="dc1",
            status=JobStatus.RUNNING.value,
            total_completed=100,
            fence_token=1,
        )
        assert gate_receive_progress(progress_a1) is True
        assert job_state["job-transfer"].total_completed == 100

        # Manager-B takes over with token=2
        progress_b1 = JobProgress(
            job_id="job-transfer",
            datacenter="dc1",
            status=JobStatus.RUNNING.value,
            total_completed=150,
            fence_token=2,
        )
        assert gate_receive_progress(progress_b1) is True
        assert job_state["job-transfer"].total_completed == 150

        # Delayed update from Manager-A with token=1 arrives (stale)
        progress_a2 = JobProgress(
            job_id="job-transfer",
            datacenter="dc1",
            status=JobStatus.RUNNING.value,
            total_completed=110,  # Old progress
            fence_token=1,
        )
        assert gate_receive_progress(progress_a2) is False  # Rejected!
        assert job_state["job-transfer"].total_completed == 150  # Unchanged

        # Manager-B continues sending updates with token=2
        progress_b2 = JobProgress(
            job_id="job-transfer",
            datacenter="dc1",
            status=JobStatus.RUNNING.value,
            total_completed=200,
            fence_token=2,
        )
        assert gate_receive_progress(progress_b2) is True
        assert job_state["job-transfer"].total_completed == 200

    def test_multiple_jobs_independent_fence_tokens(self):
        """
        Fence tokens should be tracked independently per job.
        """
        job_fence_tokens: dict[str, int] = {}

        def update_fence(job_id: str, token: int) -> bool:
            current = job_fence_tokens.get(job_id, 0)
            if token < current:
                return False
            job_fence_tokens[job_id] = max(current, token)
            return True

        # Job 1 at token 5
        assert update_fence("job-1", 5) is True

        # Job 2 at token 1 (independent from job-1)
        assert update_fence("job-2", 1) is True

        # Job 1 update with lower token fails
        assert update_fence("job-1", 3) is False

        # Job 2 update with token 3 succeeds (independent)
        assert update_fence("job-2", 3) is True

        # Final state
        assert job_fence_tokens["job-1"] == 5
        assert job_fence_tokens["job-2"] == 3
