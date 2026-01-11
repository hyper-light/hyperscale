"""
End-to-end simulation tests for fence token consistency guarantees.

These tests focus specifically on fence token invariants:
1. Concurrent leadership claims for same job - only highest token wins
2. Out-of-order message delivery - stale transfers rejected
3. Token overflow handling at boundary values
4. Verification that workers never accept lower tokens after higher ones

Fence tokens are the core correctness mechanism. This ensures the invariant
"monotonically increasing tokens" is never violated.

Tests use mocks for all networking to avoid live server requirements.
"""

import asyncio
import pytest
import random
import time
from dataclasses import dataclass, field


# =============================================================================
# Mock Infrastructure
# =============================================================================


@dataclass
class FenceTokenTransfer:
    """Represents a leadership transfer with fence token."""

    job_id: str
    workflow_ids: list[str]
    new_manager_id: str
    new_manager_addr: tuple[str, int]
    fence_token: int
    timestamp: float = field(default_factory=time.monotonic)

    def __lt__(self, other: "FenceTokenTransfer") -> bool:
        return self.fence_token < other.fence_token


@dataclass
class TransferResult:
    """Result of a transfer attempt."""

    accepted: bool
    job_id: str
    fence_token: int
    current_token: int
    reason: str = ""


class FenceTokenWorker:
    """
    Worker that enforces fence token invariants.

    This is a simplified worker that focuses on fence token validation.
    """

    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id

        # Fence token tracking per job
        self._fence_tokens: dict[str, int] = {}

        # Job leader tracking
        self._job_leaders: dict[str, tuple[str, int]] = {}

        # Workflow tracking
        self._active_workflows: set[str] = set()

        # Transfer history for verification
        self._transfer_history: list[tuple[FenceTokenTransfer, TransferResult]] = []

        # Lock for concurrent access
        self._lock = asyncio.Lock()

    def add_workflow(self, workflow_id: str, job_id: str, initial_leader: tuple[str, int]) -> None:
        """Add a workflow to track."""
        self._active_workflows.add(workflow_id)
        self._job_leaders[workflow_id] = initial_leader

    async def process_transfer(self, transfer: FenceTokenTransfer) -> TransferResult:
        """
        Process a leadership transfer.

        Enforces the fence token invariant: only accept if new token > current token.
        """
        async with self._lock:
            current_token = self._fence_tokens.get(transfer.job_id, -1)

            if transfer.fence_token <= current_token:
                result = TransferResult(
                    accepted=False,
                    job_id=transfer.job_id,
                    fence_token=transfer.fence_token,
                    current_token=current_token,
                    reason=f"Stale token: {transfer.fence_token} <= {current_token}",
                )
                self._transfer_history.append((transfer, result))
                return result

            # Accept the transfer
            self._fence_tokens[transfer.job_id] = transfer.fence_token

            # Update job leader for affected workflows
            for wf_id in transfer.workflow_ids:
                if wf_id in self._active_workflows:
                    self._job_leaders[wf_id] = transfer.new_manager_addr

            result = TransferResult(
                accepted=True,
                job_id=transfer.job_id,
                fence_token=transfer.fence_token,
                current_token=current_token,
                reason="Accepted: new token is higher",
            )
            self._transfer_history.append((transfer, result))
            return result

    def get_current_token(self, job_id: str) -> int:
        """Get current fence token for a job."""
        return self._fence_tokens.get(job_id, -1)

    def get_accepted_transfers(self) -> list[FenceTokenTransfer]:
        """Get all accepted transfers."""
        return [t for t, r in self._transfer_history if r.accepted]

    def get_rejected_transfers(self) -> list[FenceTokenTransfer]:
        """Get all rejected transfers."""
        return [t for t, r in self._transfer_history if not r.accepted]


class FenceTokenManager:
    """
    Manager that generates fence tokens for leadership transfers.

    Tracks the current token for each job and generates monotonically increasing tokens.
    """

    def __init__(self, manager_id: str, tcp_port: int) -> None:
        self.manager_id = manager_id
        self._host = "127.0.0.1"
        self._tcp_port = tcp_port

        self._job_tokens: dict[str, int] = {}
        self._is_leader = False

    def become_leader(self) -> None:
        self._is_leader = True

    def step_down(self) -> None:
        self._is_leader = False

    def get_token(self, job_id: str) -> int:
        return self._job_tokens.get(job_id, 0)

    def set_token(self, job_id: str, token: int) -> None:
        self._job_tokens[job_id] = token

    def generate_transfer(
        self,
        job_id: str,
        workflow_ids: list[str],
    ) -> FenceTokenTransfer:
        """Generate a transfer with incremented fence token."""
        current = self._job_tokens.get(job_id, 0)
        new_token = current + 1
        self._job_tokens[job_id] = new_token

        return FenceTokenTransfer(
            job_id=job_id,
            workflow_ids=workflow_ids,
            new_manager_id=self.manager_id,
            new_manager_addr=(self._host, self._tcp_port),
            fence_token=new_token,
        )

    def generate_transfer_with_token(
        self,
        job_id: str,
        workflow_ids: list[str],
        token: int,
    ) -> FenceTokenTransfer:
        """Generate a transfer with a specific token (for testing stale transfers)."""
        return FenceTokenTransfer(
            job_id=job_id,
            workflow_ids=workflow_ids,
            new_manager_id=self.manager_id,
            new_manager_addr=(self._host, self._tcp_port),
            fence_token=token,
        )


# =============================================================================
# Test Classes
# =============================================================================


class TestConcurrentLeadershipClaims:
    """
    Test concurrent leadership claims for the same job.

    Only the highest fence token should win.
    """

    @pytest.mark.asyncio
    async def test_concurrent_claims_highest_token_wins(self):
        """When multiple managers claim leadership, highest token wins."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        # Create managers with different tokens
        manager_a = FenceTokenManager("manager-a", tcp_port=9090)
        manager_b = FenceTokenManager("manager-b", tcp_port=9092)
        manager_c = FenceTokenManager("manager-c", tcp_port=9094)

        # Generate transfers with different tokens
        transfers = [
            manager_a.generate_transfer_with_token("job-001", ["wf-001"], token=3),
            manager_b.generate_transfer_with_token("job-001", ["wf-001"], token=5),
            manager_c.generate_transfer_with_token("job-001", ["wf-001"], token=4),
        ]

        # Process all concurrently
        results = await asyncio.gather(*[
            worker.process_transfer(t) for t in transfers
        ])

        # Count acceptances
        accepted = [r for r in results if r.accepted]

        # Due to concurrency, ordering varies, but final token should be 5
        assert worker.get_current_token("job-001") == 5

        # Verify the final leader
        assert worker._job_leaders["wf-001"] == ("127.0.0.1", 9092)

    @pytest.mark.asyncio
    async def test_sequential_claims_all_accepted_if_increasing(self):
        """Sequential claims with increasing tokens all accepted."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # Generate 10 sequential transfers with increasing tokens
        for i in range(1, 11):
            transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=i)
            result = await worker.process_transfer(transfer)
            assert result.accepted
            assert worker.get_current_token("job-001") == i

        # All 10 transfers should be accepted
        assert len(worker.get_accepted_transfers()) == 10

    @pytest.mark.asyncio
    async def test_rapid_concurrent_claims(self):
        """Rapid concurrent claims from multiple managers."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        # Many managers sending claims
        managers = [
            FenceTokenManager(f"manager-{i}", tcp_port=9090 + i * 2)
            for i in range(10)
        ]

        # Each manager sends transfer with its index as token
        transfers = [
            mgr.generate_transfer_with_token("job-001", ["wf-001"], token=i + 1)
            for i, mgr in enumerate(managers)
        ]

        # Shuffle to simulate network reordering
        random.shuffle(transfers)

        # Process all
        results = await asyncio.gather(*[
            worker.process_transfer(t) for t in transfers
        ])

        # Final token should be 10 (highest)
        assert worker.get_current_token("job-001") == 10

        # Some will be rejected due to concurrent processing
        rejected = [r for r in results if not r.accepted]
        accepted = [r for r in results if r.accepted]

        # At least one must be accepted (the highest eventually wins)
        assert len(accepted) >= 1


class TestOutOfOrderDelivery:
    """
    Test out-of-order message delivery.

    Stale transfers (lower tokens) must be rejected after higher tokens are accepted.
    """

    @pytest.mark.asyncio
    async def test_stale_transfer_rejected(self):
        """Transfer with lower token rejected after higher token accepted."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager_new = FenceTokenManager("manager-new", tcp_port=9092)
        manager_old = FenceTokenManager("manager-old", tcp_port=9090)

        # Accept token 5 first
        new_transfer = manager_new.generate_transfer_with_token("job-001", ["wf-001"], token=5)
        result1 = await worker.process_transfer(new_transfer)
        assert result1.accepted

        # Stale token 3 should be rejected
        old_transfer = manager_old.generate_transfer_with_token("job-001", ["wf-001"], token=3)
        result2 = await worker.process_transfer(old_transfer)

        assert not result2.accepted
        assert "Stale token" in result2.reason

        # Current token still 5
        assert worker.get_current_token("job-001") == 5

    @pytest.mark.asyncio
    async def test_equal_token_rejected(self):
        """Transfer with equal token (not greater) is rejected."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager_a = FenceTokenManager("manager-a", tcp_port=9090)
        manager_b = FenceTokenManager("manager-b", tcp_port=9092)

        # Accept token 5
        transfer_a = manager_a.generate_transfer_with_token("job-001", ["wf-001"], token=5)
        result1 = await worker.process_transfer(transfer_a)
        assert result1.accepted

        # Equal token 5 should be rejected
        transfer_b = manager_b.generate_transfer_with_token("job-001", ["wf-001"], token=5)
        result2 = await worker.process_transfer(transfer_b)

        assert not result2.accepted
        assert worker.get_current_token("job-001") == 5

    @pytest.mark.asyncio
    async def test_severely_out_of_order_delivery(self):
        """Extremely out-of-order delivery (tokens arrive in reverse order)."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # Generate transfers 1-10, deliver in reverse order
        transfers = [
            manager.generate_transfer_with_token("job-001", ["wf-001"], token=i)
            for i in range(10, 0, -1)  # 10, 9, 8, ..., 1
        ]

        results = []
        for transfer in transfers:
            result = await worker.process_transfer(transfer)
            results.append(result)

        # Only first (token 10) should be accepted
        assert results[0].accepted  # token 10

        # All others should be rejected
        for result in results[1:]:
            assert not result.accepted

        assert worker.get_current_token("job-001") == 10

    @pytest.mark.asyncio
    async def test_interleaved_accepted_and_rejected(self):
        """Interleaved pattern of accepted and rejected transfers."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # Pattern: 1, 2, 1, 3, 2, 4, 3, 5 (odd positions increase, even are stale)
        tokens = [1, 2, 1, 3, 2, 4, 3, 5]
        expected_accepted = [True, True, False, True, False, True, False, True]

        for i, token in enumerate(tokens):
            transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=token)
            result = await worker.process_transfer(transfer)
            assert result.accepted == expected_accepted[i], f"Token {token} at position {i}"

        assert worker.get_current_token("job-001") == 5


class TestTokenBoundaryValues:
    """
    Test fence token behavior at boundary values.

    Handles edge cases like zero, negative (should not happen but test robustness),
    and very large values.
    """

    @pytest.mark.asyncio
    async def test_initial_token_zero_accepted(self):
        """First transfer with token 0 is accepted (default is -1)."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=0)
        result = await worker.process_transfer(transfer)

        assert result.accepted
        assert worker.get_current_token("job-001") == 0

    @pytest.mark.asyncio
    async def test_initial_token_one_accepted(self):
        """First transfer with token 1 is accepted."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=1)
        result = await worker.process_transfer(transfer)

        assert result.accepted
        assert worker.get_current_token("job-001") == 1

    @pytest.mark.asyncio
    async def test_very_large_token_accepted(self):
        """Very large fence token is handled correctly."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        large_token = 2**62  # Very large but within int64 range

        transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=large_token)
        result = await worker.process_transfer(transfer)

        assert result.accepted
        assert worker.get_current_token("job-001") == large_token

    @pytest.mark.asyncio
    async def test_token_near_overflow(self):
        """Token near maximum int64 value is handled."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # Near max int64
        max_token = 2**63 - 1

        transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=max_token)
        result = await worker.process_transfer(transfer)

        assert result.accepted
        assert worker.get_current_token("job-001") == max_token

    @pytest.mark.asyncio
    async def test_consecutive_large_tokens(self):
        """Consecutive very large tokens work correctly."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        base = 2**60

        for i in range(5):
            transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=base + i)
            result = await worker.process_transfer(transfer)
            assert result.accepted

        assert worker.get_current_token("job-001") == base + 4


class TestMonotonicInvariant:
    """
    Test that workers never accept lower tokens after accepting higher ones.

    This is the core invariant that fence tokens provide.
    """

    @pytest.mark.asyncio
    async def test_monotonic_guarantee_sequential(self):
        """Sequential processing maintains monotonic guarantee."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # Random sequence of tokens
        tokens = [5, 2, 8, 3, 10, 1, 15, 12, 20]
        max_seen = -1

        for token in tokens:
            transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=token)
            result = await worker.process_transfer(transfer)

            if token > max_seen:
                assert result.accepted
                max_seen = token
            else:
                assert not result.accepted

            # Verify invariant: current token >= max_seen
            assert worker.get_current_token("job-001") >= max_seen

    @pytest.mark.asyncio
    async def test_monotonic_guarantee_concurrent(self):
        """Concurrent processing maintains monotonic guarantee."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        managers = [
            FenceTokenManager(f"manager-{i}", tcp_port=9090 + i * 2)
            for i in range(20)
        ]

        # Generate transfers with random tokens
        tokens = list(range(1, 21))
        random.shuffle(tokens)

        transfers = [
            managers[i].generate_transfer_with_token("job-001", ["wf-001"], token=tokens[i])
            for i in range(20)
        ]

        # Process all concurrently
        results = await asyncio.gather(*[
            worker.process_transfer(t) for t in transfers
        ])

        # Verify final token is the maximum
        assert worker.get_current_token("job-001") == 20

        # Verify all accepted transfers have tokens <= final token
        for transfer, result in zip(transfers, results):
            if result.accepted:
                assert transfer.fence_token <= worker.get_current_token("job-001")

    @pytest.mark.asyncio
    async def test_monotonic_after_many_rejections(self):
        """Monotonic guarantee holds after many rejections."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # Accept high token first
        high_transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=100)
        result = await worker.process_transfer(high_transfer)
        assert result.accepted

        # Send many lower tokens
        for i in range(50):
            low_transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=i)
            result = await worker.process_transfer(low_transfer)
            assert not result.accepted

        # Token should still be 100
        assert worker.get_current_token("job-001") == 100

        # Now send higher token - should be accepted
        higher_transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=101)
        result = await worker.process_transfer(higher_transfer)
        assert result.accepted
        assert worker.get_current_token("job-001") == 101


class TestMultiJobTokenIsolation:
    """
    Test that fence tokens are isolated per job.

    One job's token should not affect another job's token.
    """

    @pytest.mark.asyncio
    async def test_separate_token_namespaces(self):
        """Each job has independent fence token namespace."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-job1", "job-001", ("127.0.0.1", 9090))
        worker.add_workflow("wf-job2", "job-002", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # Set job-001 to token 100
        transfer1 = manager.generate_transfer_with_token("job-001", ["wf-job1"], token=100)
        await worker.process_transfer(transfer1)

        # job-002 should still accept token 1
        transfer2 = manager.generate_transfer_with_token("job-002", ["wf-job2"], token=1)
        result = await worker.process_transfer(transfer2)

        assert result.accepted
        assert worker.get_current_token("job-001") == 100
        assert worker.get_current_token("job-002") == 1

    @pytest.mark.asyncio
    async def test_concurrent_multi_job_claims(self):
        """Concurrent claims across multiple jobs don't interfere."""
        worker = FenceTokenWorker("worker-1")

        # 5 jobs, each with a workflow
        for i in range(5):
            worker.add_workflow(f"wf-{i}", f"job-{i:03d}", ("127.0.0.1", 9090))

        managers = [
            FenceTokenManager(f"manager-{i}", tcp_port=9090 + i * 2)
            for i in range(10)
        ]

        # Generate transfers for all jobs with varying tokens
        transfers = []
        for job_idx in range(5):
            for token in [3, 7, 2, 5, 10]:
                mgr = random.choice(managers)
                transfer = mgr.generate_transfer_with_token(
                    f"job-{job_idx:03d}",
                    [f"wf-{job_idx}"],
                    token=token,
                )
                transfers.append(transfer)

        # Shuffle and process
        random.shuffle(transfers)
        await asyncio.gather(*[worker.process_transfer(t) for t in transfers])

        # Each job should have final token 10
        for i in range(5):
            assert worker.get_current_token(f"job-{i:03d}") == 10


class TestTransferHistory:
    """
    Test transfer history tracking for debugging and verification.
    """

    @pytest.mark.asyncio
    async def test_history_captures_all_transfers(self):
        """Transfer history captures both accepted and rejected transfers."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # 5 increasing, 5 decreasing
        tokens = [1, 2, 3, 4, 5, 3, 2, 6, 1, 7]

        for token in tokens:
            transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=token)
            await worker.process_transfer(transfer)

        assert len(worker._transfer_history) == 10

        accepted = worker.get_accepted_transfers()
        rejected = worker.get_rejected_transfers()

        # Tokens 1,2,3,4,5,6,7 should be accepted (7 total)
        assert len(accepted) == 7
        # Tokens 3,2,1 (after higher was seen) should be rejected (3 total)
        assert len(rejected) == 3

    @pytest.mark.asyncio
    async def test_history_preserves_order(self):
        """Transfer history preserves processing order."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # Sequential processing
        tokens = [5, 3, 7, 2, 8]
        for token in tokens:
            transfer = manager.generate_transfer_with_token("job-001", ["wf-001"], token=token)
            await worker.process_transfer(transfer)

        history_tokens = [t.fence_token for t, r in worker._transfer_history]
        assert history_tokens == tokens


class TestEdgeCasesAndRobustness:
    """
    Test edge cases and robustness scenarios.
    """

    @pytest.mark.asyncio
    async def test_empty_workflow_list(self):
        """Transfer with empty workflow list still updates token."""
        worker = FenceTokenWorker("worker-1")

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        transfer = manager.generate_transfer_with_token("job-001", [], token=5)
        result = await worker.process_transfer(transfer)

        assert result.accepted
        assert worker.get_current_token("job-001") == 5

    @pytest.mark.asyncio
    async def test_unknown_workflow_in_transfer(self):
        """Transfer referencing unknown workflow doesn't fail."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # Transfer references unknown workflow
        transfer = manager.generate_transfer_with_token(
            "job-001",
            ["wf-001", "wf-unknown"],
            token=5,
        )
        result = await worker.process_transfer(transfer)

        assert result.accepted
        # Known workflow should be updated
        assert worker._job_leaders["wf-001"] == ("127.0.0.1", 9090)

    @pytest.mark.asyncio
    async def test_new_job_starts_at_negative_one(self):
        """New job defaults to token -1, so token 0 is accepted."""
        worker = FenceTokenWorker("worker-1")

        manager = FenceTokenManager("manager-a", tcp_port=9090)

        # Unknown job gets default -1
        assert worker.get_current_token("new-job") == -1

        # Token 0 should be accepted
        transfer = manager.generate_transfer_with_token("new-job", [], token=0)
        result = await worker.process_transfer(transfer)

        assert result.accepted

    @pytest.mark.asyncio
    async def test_stress_many_concurrent_transfers(self):
        """Stress test with many concurrent transfers."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        managers = [
            FenceTokenManager(f"manager-{i}", tcp_port=9090 + i)
            for i in range(100)
        ]

        # 100 concurrent transfers with random tokens
        transfers = [
            mgr.generate_transfer_with_token(
                "job-001",
                ["wf-001"],
                token=random.randint(1, 1000),
            )
            for mgr in managers
        ]

        results = await asyncio.gather(*[
            worker.process_transfer(t) for t in transfers
        ])

        # At least one should be accepted
        assert any(r.accepted for r in results)

        # Final token should be the max of all seen
        final_token = worker.get_current_token("job-001")
        max_accepted_token = max(
            t.fence_token for t, r in zip(transfers, results) if r.accepted
        )
        assert final_token == max_accepted_token

    @pytest.mark.asyncio
    async def test_rapid_sequential_same_token(self):
        """Rapid sequential transfers with same token - only first accepted."""
        worker = FenceTokenWorker("worker-1")
        worker.add_workflow("wf-001", "job-001", ("127.0.0.1", 9090))

        managers = [
            FenceTokenManager(f"manager-{i}", tcp_port=9090 + i)
            for i in range(10)
        ]

        # All send token 5
        results = []
        for mgr in managers:
            transfer = mgr.generate_transfer_with_token("job-001", ["wf-001"], token=5)
            result = await worker.process_transfer(transfer)
            results.append(result)

        # Only first should be accepted
        assert results[0].accepted
        assert all(not r.accepted for r in results[1:])