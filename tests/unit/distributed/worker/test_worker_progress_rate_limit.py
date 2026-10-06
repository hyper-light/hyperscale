"""
Worker progress under an AD-24 rate-limit refusal from its job leader.

A manager that refuses a progress update answers with a
``RateLimitResponse``. That is backpressure from a live manager, not a
failure: the worker must not trip the manager's circuit, must not fan the
update out to other managers, and must not send that manager progress
again until the refusal's retry-after passes. Progress snapshots are
cumulative, so the skipped sends lose nothing.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.models import (
    RateLimitResponse,
    WorkflowProgress,
)
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.worker.config import WorkerConfig
from hyperscale.distributed.nodes.worker.progress import WorkerProgressReporter
from hyperscale.distributed.nodes.worker.registry import WorkerRegistry
from hyperscale.distributed.nodes.worker.state import WorkerState

JOB_LEADER_ADDR = ("10.0.0.2", 9000)
WORKFLOW_ID = "workflow-1"


class RecordingSendTcp:
    """send_tcp double: answers every request with ``response``."""

    def __init__(self, response: bytes) -> None:
        self.response = response
        self.targets: list[tuple[str, int]] = []

    async def __call__(
        self,
        target: tuple[str, int],
        action: str,
        payload: bytes,
        timeout: float,
    ) -> tuple[bytes, float]:
        self.targets.append(target)
        return self.response, 0.0


def make_reporter() -> tuple[WorkerProgressReporter, WorkerRegistry]:
    logger = MagicMock()
    logger.log = AsyncMock()
    registry = WorkerRegistry(logger, circuit_breaker_config=Env().get_circuit_breaker_config(), select_manager=lambda manager_ids: None)
    state = WorkerState(
        core_allocator=MagicMock(),
        throughput_interval_seconds=Env().WORKER_THROUGHPUT_INTERVAL_SECONDS,
        completion_times_max_samples=Env().WORKER_COMPLETION_TIMES_MAX_SAMPLES,
    )
    state.set_workflow_job_leader(WORKFLOW_ID, JOB_LEADER_ADDR)
    config = WorkerConfig.from_env(env=Env(), host="127.0.0.1", tcp_port=9000, udp_port=9001)
    return WorkerProgressReporter(registry=registry, state=state, config=config, logger=logger), registry


def make_progress(completed_count: int) -> WorkflowProgress:
    return WorkflowProgress(
        job_id="job-1",
        workflow_id=WORKFLOW_ID,
        workflow_name="SimWorkflow",
        status="running",
        completed_count=completed_count,
        failed_count=0,
        rate_per_second=0.0,
        elapsed_seconds=0.0,
    )


def refusal(retry_after_seconds: float) -> bytes:
    return RateLimitResponse(
        operation="workflow_progress",
        retry_after_seconds=retry_after_seconds,
    ).dump()


async def send(
    reporter: WorkerProgressReporter,
    send_tcp: RecordingSendTcp,
    completed_count: int,
) -> bool:
    return await reporter.send_progress_to_job_leader(
        progress=make_progress(completed_count),
        send_tcp=send_tcp,
        node_host="127.0.0.1",
        node_port=9100,
        node_id_short="worker-1",
    )


@pytest.mark.asyncio
async def test_refusal_holds_sends_to_that_manager_until_retry_after() -> None:
    reporter, _registry = make_reporter()
    send_tcp = RecordingSendTcp(refusal(retry_after_seconds=60.0))

    assert await send(reporter, send_tcp, completed_count=1) is True
    assert await send(reporter, send_tcp, completed_count=2) is True

    assert send_tcp.targets == [JOB_LEADER_ADDR]


@pytest.mark.asyncio
async def test_refusals_never_trip_the_circuit_or_fan_out() -> None:
    reporter, registry = make_reporter()
    send_tcp = RecordingSendTcp(refusal(retry_after_seconds=0.0))
    circuit = registry.get_or_create_circuit_by_addr(JOB_LEADER_ADDR)
    refusal_count = 4 * (circuit.error_threshold or circuit.max_errors)

    for completed_count in range(refusal_count):
        assert await send(reporter, send_tcp, completed_count) is True

    assert send_tcp.targets == [JOB_LEADER_ADDR] * refusal_count
    assert not circuit.is_open()


@pytest.mark.asyncio
async def test_expired_refusal_is_released() -> None:
    reporter, _registry = make_reporter()
    send_tcp = RecordingSendTcp(refusal(retry_after_seconds=0.0))

    await send(reporter, send_tcp, completed_count=1)
    assert JOB_LEADER_ADDR in reporter._refused_until

    assert reporter._is_refusing(JOB_LEADER_ADDR) is False
    assert reporter._refused_until == {}
