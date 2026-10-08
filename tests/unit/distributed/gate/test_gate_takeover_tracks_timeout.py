"""
A gate that takes a job over tracks its global timeout (AD-34).

Global-timeout tracking started when a gate dispatched a job, or recovered
it from its ledger -- never when it took a job over from a leader gate that
died. The new leader then never timed the job out: stalled, it ran on
forever. A takeover now tracks the job for what is left of its budget.

A real ``GateServer`` (never started) leads the gate cluster alone; the
election and the task runner of a started gate are stood in for. It holds
a job submitted 20 of its 60 seconds ago, led by a gate that died.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import GlobalJobStatus, JobStatus, JobSubmission
from hyperscale.distributed.nodes.gate.server import GateServer

JOB_ID = "job-1"
TIMEOUT_SECONDS = 60.0
ELAPSED_SECONDS = 20.0
# The two clock reads (test, takeover) a moment apart.
TOLERANCE_SECONDS = 1.0


def make_gate() -> GateServer:
    gate = GateServer(
        host="127.0.0.1",
        tcp_port=19161,
        udp_port=19162,
        env=Env(MERCURY_SYNC_AUTH_SECRET="takeover-timeout-test-secret-01234567"),
        datacenter_managers={"dc-a": [("127.0.0.1", 19261)]},
        datacenter_manager_udp={"dc-a": [("127.0.0.1", 19262)]},
    )
    gate.is_leader = lambda: True
    gate._task_runner = SimpleNamespace(run=lambda *args, **kwargs: None)
    return gate


@pytest.mark.asyncio
async def test_a_taken_over_job_is_tracked_for_what_is_left_of_its_budget() -> None:
    gate = make_gate()
    gate._job_manager.set_job(
        JOB_ID,
        GlobalJobStatus(
            job_id=JOB_ID,
            status=JobStatus.RUNNING.value,
            timestamp=gate._clock.monotonic() - ELAPSED_SECONDS,
        ),
    )
    gate._job_manager.set_target_dcs(JOB_ID, {"dc-a"})
    gate._modular_state._job_submissions[JOB_ID] = JobSubmission(
        job_id=JOB_ID,
        workflows=b"",
        vus=1,
        timeout_seconds=TIMEOUT_SECONDS,
    )

    fence_token = await gate._commit_gate_job_leadership_takeover(JOB_ID)

    tracked = gate._job_timeout_tracker._tracked_jobs.get(JOB_ID)
    assert fence_token is not None
    assert tracked is not None
    assert abs(tracked.timeout_seconds - (TIMEOUT_SECONDS - ELAPSED_SECONDS)) < TOLERANCE_SECONDS
    assert tracked.target_datacenters == ["dc-a"]
