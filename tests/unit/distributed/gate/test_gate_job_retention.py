"""
A gate keeps a finished job for its retention after the job ended.

The gate swept a terminal job once its *timestamp* was older than the
retention (FAILED_JOB_MAX_AGE): that timestamp is the job's submission, so
a job that ran longer than the retention went the moment it ended -- its
status unanswerable and its late results unrecognized at once. (Progress
also overwrote the timestamp with the time of its latest report, which hid
this for jobs reporting to the end, and misreported their elapsed time.)
Retention now runs from when the gate first found the job terminal, as the
manager's runs from a job's completion.

A real ``GateServer`` (never started) holds a job submitted well beyond
the retention ago, now terminal.
"""

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import GlobalJobStatus, JobStatus
from hyperscale.distributed.nodes.gate.server import GateServer

JOB_ID = "job-1"


def make_gate() -> GateServer:
    return GateServer(
        host="127.0.0.1",
        tcp_port=19121,
        udp_port=19122,
        env=Env(MERCURY_SYNC_AUTH_SECRET="job-retention-test-secret-0123456789"),
        datacenter_managers={"dc-a": [("127.0.0.1", 19221)]},
        datacenter_manager_udp={"dc-a": [("127.0.0.1", 19222)]},
    )


@pytest.mark.asyncio
async def test_a_finished_job_is_kept_for_its_retention_after_it_ended() -> None:
    gate = make_gate()
    ended_at = 10 * gate._job_max_age
    gate._job_manager.set_job(
        JOB_ID,
        GlobalJobStatus(job_id=JOB_ID, status=JobStatus.FAILED.value, timestamp=1.0),
    )

    swept_when_found_ended = gate._get_expired_terminal_jobs(ended_at)
    swept_within_retention = gate._get_expired_terminal_jobs(ended_at + gate._job_max_age)
    swept_after_retention = gate._get_expired_terminal_jobs(ended_at + gate._job_max_age + 1.0)

    assert (swept_when_found_ended, swept_within_retention, swept_after_retention) == ([], [], [JOB_ID])
