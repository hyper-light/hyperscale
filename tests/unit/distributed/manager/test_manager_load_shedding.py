"""
AD-22 load shedding on the manager's job submission path.

The manager ran its own ManagerLoadShedder: its overload tracker was
never fed (nothing called on_request_start/end), so it never shed; and
had it shed, the handler called ``get_current_state`` -- a method that
shedder did not have -- and raised. The manager now sheds through the
shared reliability LoadShedder over the overload detector its resource
sampler feeds, as the gate does.

Its resource sampler is the detector's only sampler; shedding checks
read the state it settled on. (Each check used to sample the detector
with zero CPU and memory, so a burst of submissions counted toward the
detector's de-escalation hysteresis and talked a CPU-overloaded node out
of shedding.)

Driven through the manager's real ``job_submission`` handler: an
overloaded node refuses the submission with the load reason (a
transient error the client retries) -- and keeps refusing through a
burst of submissions; a quiet one does not shed.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import JobAck
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.protocol.transient_errors import TRANSIENT_ERRORS
from hyperscale.distributed.reliability.load_shedding import LoadShedder
from hyperscale.distributed.reliability.overload import HybridOverloadDetector

QUIET_LATENCY_MS = 10.0
OVERLOAD_LATENCY_MS = 5_000.0
LATENCY_SAMPLES = 200
OVERLOADED_CPU_PERCENT = 99.0
SUBMISSION_BURST = 100


class AllowAll:
    async def check_rate_limit(self, client_id: str, operation: str):
        return SimpleNamespace(allowed=True, retry_after_seconds=0.0)


def make_manager(latency_ms: float, cpu_percent: float = 0.0) -> ManagerServer:
    detector = HybridOverloadDetector()
    for _ in range(LATENCY_SAMPLES):
        detector.record_latency(latency_ms)
    detector.get_state(cpu_percent, 0.0)  # the resource sampler's tick
    manager = object.__new__(ManagerServer)
    manager._rate_limiter = AllowAll()
    manager._load_shedder = LoadShedder(detector, detector_sampled_externally=True)
    manager._udp_logger = SimpleNamespace(log=None)
    manager._task_runner = SimpleNamespace(run=lambda *args, **kwargs: None)
    manager._host, manager._tcp_port = "127.0.0.1", 9000
    manager._node_id = SimpleNamespace(short="manager-a", full="manager-a-full")
    return manager


@pytest.mark.asyncio
async def test_an_overloaded_manager_sheds_submissions_with_a_transient_refusal() -> None:
    manager = make_manager(OVERLOAD_LATENCY_MS)

    ack = JobAck.load(await ManagerServer.job_submission(manager, ("127.0.0.1", 9500), b"not parsed", 0))

    assert ack.accepted is False
    assert ack.error == "System under load (overloaded), please retry later"
    assert any(marker in ack.error.lower() for marker in TRANSIENT_ERRORS)


@pytest.mark.asyncio
async def test_a_burst_of_submissions_does_not_talk_a_cpu_overloaded_manager_out_of_shedding() -> None:
    manager = make_manager(QUIET_LATENCY_MS, cpu_percent=OVERLOADED_CPU_PERCENT)

    acks = [
        JobAck.load(await ManagerServer.job_submission(manager, ("127.0.0.1", 9500), b"not parsed", 0))
        for _ in range(SUBMISSION_BURST)
    ]

    assert all(ack.error == "System under load (overloaded), please retry later" for ack in acks)


@pytest.mark.asyncio
async def test_a_quiet_manager_does_not_shed() -> None:
    manager = make_manager(QUIET_LATENCY_MS)
    assert manager._load_shedder.should_shed_handler("job_submission") is False
