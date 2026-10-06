"""
AD-38 Part 8 at the client: a SESSION read of a job the client tracks
carries back the newest view of it a read answered -- so no node may
answer it with anything older -- and the client forgets that view with
the job. A job it does not track leaves nothing behind.
"""

import pytest

from hyperscale.distributed.models import GlobalJobStatus, JobStatusQuery, ReadConsistency
from hyperscale.distributed.models.client import ClientJobResult
from hyperscale.distributed.nodes.client.client import HyperscaleClient
from hyperscale.distributed.nodes.client.state import ClientState

NODE = ("10.0.0.1", 9000)


def make_client(answers: list[GlobalJobStatus]) -> tuple[HyperscaleClient, list[JobStatusQuery]]:
    sent: list[JobStatusQuery] = []
    client = object.__new__(HyperscaleClient)
    client._state = ClientState()

    async def send_tcp(addr, action, payload, timeout):
        sent.append(JobStatusQuery.load(payload))
        return answers.pop(0).dump(), 0

    client.send_tcp = send_tcp
    return client, sent


@pytest.mark.asyncio
async def test_a_session_read_carries_back_the_newest_view_seen() -> None:
    client, sent = make_client(
        [
            GlobalJobStatus(job_id="job-1", status="running", fence_token=2, view_time=50.0),
            GlobalJobStatus(job_id="job-1", status="running", fence_token=2, view_time=40.0),
            GlobalJobStatus(job_id="job-1", status="running", fence_token=2, view_time=60.0),
        ]
    )
    client._state.initialize_job_tracking("job-1", ClientJobResult(job_id="job-1", status="submitted"))

    for _ in range(3):
        await client.query_job_status(NODE, "job-1", 1.0, consistency=ReadConsistency.SESSION)

    assert [(query.observed_fence_token, query.observed_view_time) for query in sent] == [
        (0, 0.0),
        (2, 50.0),
        (2, 50.0),  # an older answer never moves the session back
    ]
    assert client._state.get_job_read_view("job-1") == (2, 60.0)

    client._state.release_job("job-1")
    assert client._state.get_job_read_view("job-1") == (0, 0.0)


@pytest.mark.asyncio
async def test_an_untracked_job_leaves_no_view_behind() -> None:
    client, _sent = make_client([GlobalJobStatus(job_id="job-x", status="running", fence_token=1, view_time=5.0)])

    await client.query_job_status(NODE, "job-x", 1.0, consistency=ReadConsistency.SESSION)

    assert client._state._job_read_views == {}
