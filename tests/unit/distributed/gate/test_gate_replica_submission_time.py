"""
A job's submission time crosses gates as wall-clock time.

The takeover capsule (``GateJobReplica``) carried the submission instant
as the accepting gate's *monotonic* reading, and every peer stored it as
its own. Monotonic clocks have a per-host epoch: on any other host that
number is meaningless, so a peer's -- and, after a takeover, the new
leader's -- elapsed time for the job was off by the difference of two
hosts' uptimes, and so was everything measured from it.

The replica now carries the wall-clock submission time (AD-39 bounds the
gates' wall-clock disagreement), and each gate converts it to its own
monotonic base. Two real ``GateServer`` instances (never started) whose
monotonic clocks have epochs a long uptime apart: a job submitted on one
30 seconds ago reads as 30 seconds old on the other.
"""

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models.gate_replication import GateJobReplica
from hyperscale.distributed.models import JobStatus
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.runtime import RealClock

JOB_ID = "job-1"
SUBMITTED_SECONDS_AGO = 30.0
# Another host booted this much earlier: its monotonic clock reads that
# much more at the same instant.
UPTIME_DIFFERENCE_SECONDS = 1_000_000.0
# Wall time read twice, a moment apart, while the test runs.
TOLERANCE_SECONDS = 1.0


class ShiftedMonotonicClock:
    """The real clock, its monotonic epoch shifted as another host's is."""

    def __init__(self, shift_seconds: float) -> None:
        self._clock = RealClock()
        self._shift_seconds = shift_seconds

    def monotonic(self) -> float:
        return self._clock.monotonic() + self._shift_seconds

    def monotonic_ns(self) -> int:
        return self._clock.monotonic_ns() + int(self._shift_seconds * 1_000_000_000)

    def time(self) -> float:
        return self._clock.time()

    async def sleep(self, seconds: float) -> None:
        await self._clock.sleep(seconds)

    async def wait_for(self, awaitable, timeout: float | None):
        return await self._clock.wait_for(awaitable, timeout)


def make_gate(tcp_port: int, clock: ShiftedMonotonicClock) -> GateServer:
    return GateServer(
        host="127.0.0.1",
        tcp_port=tcp_port,
        udp_port=tcp_port + 1,
        env=Env(MERCURY_SYNC_AUTH_SECRET="replica-submission-test-secret-012345"),
        datacenter_managers={"dc-a": [("127.0.0.1", 19231)]},
        datacenter_manager_udp={"dc-a": [("127.0.0.1", 19232)]},
        clock=clock,
    )


@pytest.mark.asyncio
async def test_a_replica_carries_the_submission_time_across_monotonic_epochs() -> None:
    accepting_gate = make_gate(19131, ShiftedMonotonicClock(0.0))
    peer_gate = make_gate(19141, ShiftedMonotonicClock(UPTIME_DIFFERENCE_SECONDS))
    accepting_address = ("127.0.0.1", 19131)

    await peer_gate._apply_committed_replica(
        GateJobReplica(
            job_id=JOB_ID,
            sequence=1,
            fence_token=1,
            leader_id="gate-accepting",
            leader_addr=accepting_address,
            origin_gate_addr=accepting_address,
            callback_addr=None,
            target_dcs=["dc-a"],
            target_dc_count=1,
            status_seed=JobStatus.SUBMITTED.value,
            submitted_wall_time=accepting_gate._clock.time() - SUBMITTED_SECONDS_AGO,
            raft_voters=[accepting_gate._node_id.full, peer_gate._node_id.full],
        )
    )

    elapsed_on_peer = peer_gate._clock.monotonic() - peer_gate._job_manager.get_job(JOB_ID).timestamp
    assert abs(elapsed_on_peer - SUBMITTED_SECONDS_AGO) < TOLERANCE_SECONDS
