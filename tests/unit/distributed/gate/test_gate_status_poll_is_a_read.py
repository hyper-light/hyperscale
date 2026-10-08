"""
A client's status poll reads the job; it does not decide it.

The poll aggregated the datacenters' final results into the stored job:
with one datacenter failed and another still running, it resolved the
job FAILED -- and the datacenter still running "timed out" -- and stored
that, with no terminal path run: the paths that finalize a job pass over
one already terminal, so its requestor never got its result. It also
overwrote the job's live totals and rate, kept by progress, with sums of
final results -- zero until datacenters finished.

A real ``GateServer`` (never started) holds a running job whose live
totals came from progress; one target datacenter reported a failed final
result, the other is still running. The poll answers the job as held --
running, its live totals, the failed datacenter tallied with its error --
and the stored job is unchanged.
"""

import dataclasses

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import GlobalJobStatus, JobFinalResult, JobStatus
from hyperscale.distributed.nodes.gate.server import GateServer

JOB_ID = "job-1"


def make_gate() -> GateServer:
    return GateServer(
        host="127.0.0.1",
        tcp_port=19111,
        udp_port=19112,
        env=Env(MERCURY_SYNC_AUTH_SECRET="status-poll-test-secret-0123456789"),
        datacenter_managers={
            "dc-a": [("127.0.0.1", 19211)],
            "dc-b": [("127.0.0.1", 19311)],
        },
        datacenter_manager_udp={
            "dc-a": [("127.0.0.1", 19212)],
            "dc-b": [("127.0.0.1", 19312)],
        },
    )


@pytest.mark.asyncio
async def test_a_status_poll_reports_the_job_without_resolving_it() -> None:
    gate = make_gate()
    running_job = GlobalJobStatus(
        job_id=JOB_ID,
        status=JobStatus.RUNNING.value,
        total_completed=50,
        total_failed=2,
        overall_rate=12.5,
        timestamp=gate._clock.monotonic(),
    )
    gate._job_manager.set_job(JOB_ID, running_job)
    gate._job_manager.set_target_dcs(JOB_ID, {"dc-a", "dc-b"})
    gate._job_manager.set_dc_result(
        JOB_ID,
        "dc-a",
        JobFinalResult(
            job_id=JOB_ID,
            datacenter="dc-a",
            status=JobStatus.FAILED.value,
            total_completed=10,
            total_failed=5,
            errors=["workers lost"],
        ),
    )
    stored_before = dataclasses.replace(gate._job_manager.get_job(JOB_ID))

    polled = await gate._gather_job_status(JOB_ID)

    assert (polled.status, polled.total_completed, polled.total_failed, polled.overall_rate) == (
        JobStatus.RUNNING.value,
        50,
        2,
        12.5,
    )
    assert (polled.completed_datacenters, polled.failed_datacenters, polled.errors) == (
        0,
        1,
        ["dc-a: workers lost"],
    )
    assert gate._job_manager.get_job(JOB_ID) == stored_before
