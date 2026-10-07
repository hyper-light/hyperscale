"""AD-20/AD-37: an overloaded gate or manager still admits cancellation.

AD-37 classes cancellation as CONTROL, "never backpressured (CRITICAL)",
and ``classify_handler`` (``reliability/message_class.py``) puts
``cancel_job`` and ``receive_cancel_single_workflow`` (and the manager's
``extension_request``) in that class. At base commit 2e6d0532 the handlers'
own AD-24 checks ran at NORMAL whatever the handler
(``nodes/gate/server.py:6246-6253``, ``nodes/manager/server.py:6310``,
``nodes/manager/cancellation.py:1828`` -> ``server_rate_limiter.py:175-177``),
and ``adaptive_rate_limiter.py:131`` refuses every non-CRITICAL request
when OVERLOADED: an overloaded node refused the cancel meant to relieve it.

Each test drives a real ``ServerRateLimiter`` whose detector is OVERLOADED,
the level at which NORMAL traffic is shed.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from hyperscale.distributed.models import CancelJob, HealthcheckExtensionRequest, SingleWorkflowCancelRequest
from hyperscale.distributed.nodes.gate.handlers.tcp_cancellation import GateCancellationHandler
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.manager.cancellation import ManagerCancellationCoordinator
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.reliability.overload import HybridOverloadDetector, OverloadState
from hyperscale.distributed.reliability.priority import RequestPriority
from hyperscale.distributed.reliability.rate_limit_result import RateLimitResult
from hyperscale.distributed.reliability.rate_limiting import ServerRateLimiter

CLIENT_ADDR = ("10.0.0.9", 7000)
CLIENT_ID = f"{CLIENT_ADDR[0]}:{CLIENT_ADDR[1]}"
QUIET_LATENCY_MS = 10.0
LATENCY_SAMPLES = 200
OVERLOADED_CPU_PERCENT = 99.0


class RateCheckRecorded(Exception):
    """Stops a handler right after its rate check, so only the check is under test."""


def overloaded_rate_limiter() -> ServerRateLimiter:
    """A node's limiter over a detector its resource sampler left OVERLOADED."""
    detector = HybridOverloadDetector()
    for _ in range(LATENCY_SAMPLES):
        detector.record_latency(QUIET_LATENCY_MS)
    detector.get_state(OVERLOADED_CPU_PERCENT, 0.0)
    assert detector.current_state is OverloadState.OVERLOADED
    return ServerRateLimiter(overload_detector=detector, detector_sampled_externally=True)


class RecordingRateLimiter:
    """Delegates to an overloaded limiter, records each verdict, then stops the handler."""

    def __init__(self) -> None:
        self.rate_limiter = overloaded_rate_limiter()
        self.verdicts: list[tuple[str, RateLimitResult]] = []

    async def check_rate_limit(self, client_id: str, operation: str) -> RateLimitResult:
        self.verdicts.append((operation, await self.rate_limiter.check_rate_limit(client_id, operation)))
        raise RateCheckRecorded()

    async def check_rate_limit_with_priority(
        self, client_id: str, operation: str, priority: RequestPriority
    ) -> RateLimitResult:
        self.verdicts.append(
            (operation, await self.rate_limiter.check_rate_limit_with_priority(client_id, operation, priority))
        )
        raise RateCheckRecorded()


class RecordingNodeRateCheck:
    """A node's ``_check_rate_limit_for_operation`` over an overloaded limiter,
    recording each verdict and then stopping the handler."""

    def __init__(self, node_class: type[GateServer] | type[ManagerServer]) -> None:
        self.node_stub = SimpleNamespace(_rate_limiter=overloaded_rate_limiter())
        self.node_class = node_class
        self.verdicts: list[tuple[str, bool]] = []

    async def __call__(self, client_id: str, operation: str, handler_name: str) -> tuple[bool, float]:
        allowed, _ = await self.node_class._check_rate_limit_for_operation(
            self.node_stub, client_id, operation, handler_name
        )
        self.verdicts.append((operation, allowed))
        raise RateCheckRecorded()


@pytest.mark.asyncio
@pytest.mark.parametrize("node_class", [GateServer, ManagerServer])
async def test_the_overload_level_sheds_normal_traffic(node_class: type[GateServer] | type[ManagerServer]) -> None:
    node_stub = SimpleNamespace(_rate_limiter=overloaded_rate_limiter())

    allowed, _ = await node_class._check_rate_limit_for_operation(
        node_stub, CLIENT_ID, "stats_update", "windowed_stats_push"
    )

    assert allowed is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("node_class", "operation", "handler_name"),
    [
        (GateServer, "cancel", "cancel_job"),
        (GateServer, "cancel_workflow", "receive_cancel_single_workflow"),
        (ManagerServer, "cancel", "cancel_job"),
        (ManagerServer, "extension", "extension_request"),
    ],
)
async def test_node_rate_check_admits_control_handlers_when_overloaded(
    node_class: type[GateServer] | type[ManagerServer], operation: str, handler_name: str
) -> None:
    node_stub = SimpleNamespace(_rate_limiter=overloaded_rate_limiter())

    allowed, _ = await node_class._check_rate_limit_for_operation(node_stub, CLIENT_ID, operation, handler_name)

    assert allowed is True


def gate_cancellation_handler(rate_check: RecordingNodeRateCheck) -> GateCancellationHandler:
    """A gate cancellation handler whose only live collaborator is its rate check."""
    return GateCancellationHandler(
        state=MagicMock(),
        logger=MagicMock(),
        task_runner=MagicMock(),
        job_manager=MagicMock(),
        datacenter_managers={},
        get_node_id=MagicMock(),
        get_host=lambda: "127.0.0.1",
        get_tcp_port=lambda: 9000,
        check_rate_limit=rate_check,
        send_tcp=MagicMock(),
        record_cancellation=MagicMock(),
        client_push_timeout_seconds=1.0,
        manager_request_timeout_seconds=1.0,
    )


@pytest.mark.asyncio
async def test_gate_admits_a_job_cancel_when_overloaded() -> None:
    rate_check = RecordingNodeRateCheck(GateServer)
    handler = gate_cancellation_handler(rate_check)

    with pytest.raises(RateCheckRecorded):
        await handler._cancel_job_for_client(CLIENT_ADDR, CancelJob(job_id="job-1", reason="user").dump())

    assert rate_check.verdicts == [("cancel", True)]


@pytest.mark.asyncio
async def test_gate_admits_a_workflow_cancel_when_overloaded() -> None:
    rate_check = RecordingNodeRateCheck(GateServer)
    handler = gate_cancellation_handler(rate_check)
    request = SingleWorkflowCancelRequest(
        job_id="job-1", workflow_id="workflow-1", request_id="request-1", requester_id="client-1", timestamp=0.0
    )

    with pytest.raises(RateCheckRecorded):
        await handler._cancel_single_workflow(CLIENT_ADDR, request.dump())

    assert rate_check.verdicts == [("cancel_workflow", True)]


@pytest.mark.asyncio
async def test_manager_admits_a_job_cancel_when_overloaded() -> None:
    rate_check = RecordingNodeRateCheck(ManagerServer)
    coordinator = object.__new__(ManagerCancellationCoordinator)
    coordinator._check_rate_limit_for_operation = rate_check

    with pytest.raises(RateCheckRecorded):
        await coordinator._cancel_job(CLIENT_ADDR, CancelJob(job_id="job-1", reason="user").dump())

    assert rate_check.verdicts == [("cancel", True)]


@pytest.mark.asyncio
async def test_manager_admits_a_workflow_cancel_when_overloaded() -> None:
    recording_rate_limiter = RecordingRateLimiter()
    coordinator = object.__new__(ManagerCancellationCoordinator)
    coordinator._rate_limiter = recording_rate_limiter
    request = SingleWorkflowCancelRequest(
        job_id="job-1", workflow_id="workflow-1", request_id="request-1", requester_id="client-1", timestamp=0.0
    )

    with pytest.raises(RateCheckRecorded):
        await coordinator._cancel_single_workflow(CLIENT_ADDR, request.dump())

    assert [(operation, verdict.allowed) for operation, verdict in recording_rate_limiter.verdicts] == [
        ("cancel_workflow", True)
    ]


@pytest.mark.asyncio
async def test_manager_admits_an_extension_request_when_overloaded() -> None:
    rate_check = RecordingNodeRateCheck(ManagerServer)
    manager_stub = SimpleNamespace(_check_rate_limit_for_operation=rate_check)
    request = HealthcheckExtensionRequest(
        worker_id="worker-1",
        reason="long step",
        current_progress=1.0,
        estimated_completion=1.0,
        active_workflow_count=1,
    )

    with pytest.raises(RateCheckRecorded):
        await ManagerServer._handle_extension_request(manager_stub, CLIENT_ADDR, request.dump())

    assert rate_check.verdicts == [("extension", True)]
