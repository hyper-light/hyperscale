"""
A job id's submission is decided once, by one request at a time -- on real
managers, on virtual time.

A submission whose answer is lost or late is retried: the gate re-sends it
when its dispatch times out, and a client re-sends its own. The manager
re-ran the whole submission for a job it already held: ``create_job``
returned the existing job and the handler carried on, resetting the job's
fence to 1, restarting its timeout, writing a second ledger record over the
first and registering its workflows again over the ones running. A retry
that arrived while the first attempt was still being decided ran the
submission concurrently with it. And a keyed retry finding its key still
reserved was answered "Request pending, please retry" -- a phrase outside
the shared transient vocabulary, so the gate took it for a refusal and
dispatched the job to another datacenter while this one ran it.

A submission that failed after the job was created -- its quorum
replication failing under a partition, say -- answered with a refusal and
left the job behind: in memory with its timeout running, leased to this
manager, recorded ACTIVE in the ledger and persisted for resume, so a
restart brought back a job its submitter was told was refused.

Three real ``ManagerServer`` instances form a datacenter on a
``SimulationLoop``; the one stand-in is the worker, registered through the
leader's own registration handler (nothing here dispatches to it). The
link from the leader to its peers is delayed or cut to hold a submission
mid-decision, or to fail it:

* a resubmitted job is answered from the job, and nothing of it is redone;
  a different job under the same id is refused; a follower holding the job
  answers for it too;
* a resubmission (keyed or not) while the job is being decided is told to
  retry, in words the gate and the client retry;
* a submission refused before dispatch leaves nothing behind -- in memory,
  in its lease, in the ledger (closed FAILED) or on disk -- and its retry
  is decided afresh.
"""

import asyncio
import sys

import cloudpickle
import pytest

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import step
from hyperscale.distributed.models import JobAck
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.protocol.transient_errors import is_transient_rejection

from .leader_to_peer_link import LeaderToPeerLink
from .manager_datacenter import (
    ManagerBuilder,
    CLIENT_ADDRESS,
    MANAGER_TCP_ADDRESSES,
    Checkout,
    form_datacenter,
    register_worker,
    run_scenario,
    submission_of,
    submit,
)

cloudpickle.register_pickle_by_value(sys.modules[__name__])

# Below the managers' short TCP timeout (2s): the replication it delays
# still succeeds, after holding the submission undecided.
REPLICATION_DELAY_SECONDS = 0.5


class Browse(Workflow):
    vus = 1

    @step()
    async def browse(self) -> dict:
        return {}


def test_a_resubmitted_job_is_answered_from_the_job_without_redoing_it() -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        submission = submission_of("job-1", Checkout())
        first_ack = await submit(leader, submission)

        job = leader._job_manager.get_job_by_id("job-1")
        accepted_state = (
            job.fencing_token,
            job.timeout_tracking.started_at,
            dict(job.workflows),
            leader._leases.get_fence_token("job-1"),
        )
        await asyncio.sleep(1.0)
        retry_ack = await submit(leader, submission)
        other_job_ack = await submit(leader, submission_of("job-1", Browse()))
        [follower, _] = [manager for manager in managers if manager is not leader]
        follower_ack = await submit(follower, submission)

        held_job = leader._job_manager.get_job_by_id("job-1")
        held_state = (
            held_job.fencing_token,
            held_job.timeout_tracking.started_at,
            dict(held_job.workflows),
            leader._leases.get_fence_token("job-1"),
        )
        return first_ack, retry_ack, other_job_ack, follower_ack, job, held_job, accepted_state, held_state

    first_ack, retry_ack, other_job_ack, follower_ack, job, held_job, accepted_state, held_state = (
        run_scenario(scenario, link)
    )

    assert (first_ack.accepted, retry_ack.accepted, follower_ack.accepted) == (True, True, True)
    assert held_job is job
    # Same fence, same timeout start, the very workflow records, same lease.
    assert held_state[0] == accepted_state[0] == 1
    assert held_state[1] == accepted_state[1]
    assert held_state[2].keys() == accepted_state[2].keys()
    assert all(held_state[2][token] is accepted_state[2][token] for token in accepted_state[2])
    assert held_state[3] == accepted_state[3]
    assert (other_job_ack.accepted, other_job_ack.error) == (
        False,
        "Job id job-1 is in use by another job",
    )
    assert not is_transient_rejection(other_job_ack.error)


@pytest.mark.parametrize("idempotency_key", [None, "client-a:0:feedface"])
def test_a_resubmission_while_the_job_is_decided_is_told_to_retry(idempotency_key: str | None) -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        submission = submission_of("job-1", Checkout(), idempotency_key)
        # The first attempt waits on its announcement and replication to
        # the peers, holding the job id undecided.
        link.delay_seconds = REPLICATION_DELAY_SECONDS
        first_attempt = asyncio.ensure_future(leader.job_submission(CLIENT_ADDRESS, submission, 0))
        await asyncio.sleep(REPLICATION_DELAY_SECONDS / 5)
        concurrent_ack = await submit(leader, submission)
        first_ack = JobAck.load(await first_attempt)
        link.delay_seconds = 0.0
        later_ack = await submit(leader, submission)
        return concurrent_ack, first_ack, later_ack, leader._job_submissions_in_progress

    concurrent_ack, first_ack, later_ack, in_progress = run_scenario(scenario, link)

    assert (concurrent_ack.accepted, concurrent_ack.error) == (False, "submission in progress, retry")
    assert is_transient_rejection(concurrent_ack.error)
    assert (first_ack.accepted, later_ack.accepted) == (True, True)
    assert in_progress == set()


def test_a_submission_refused_before_dispatch_leaves_nothing_behind() -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        submission = submission_of("job-1", Checkout())
        payload_path = leader._config.wal_data_dir / "submissions" / "job-1.bin"

        # Cut from its peers, the leader cannot replicate the job to a
        # quorum before dispatch: the submission is refused.
        link.cut = True
        refused_ack = await submit(leader, submission)
        left_behind = (
            leader._job_manager.get_job_by_id("job-1"),
            leader._leases.get_job_leader("job-1"),
            leader._manager_state.get_job_timeout_strategy("job-1"),
            leader._manager_state.get_job_submission("job-1"),
            [key for key in leader._workflow_dispatcher._pending if key.startswith("job-1:")],
            set(leader._job_submissions_in_progress),
        )
        refused_ledger_record = leader._job_ledger.get_job("job-1")
        refused_payload_kept = await leader._storage_filesystem.exists(payload_path)

        # Healed, the retry is decided afresh. The cut outlasted the
        # stand-in worker's SWIM life, so a fresh one registers.
        link.cut = False
        await register_worker(leader, "worker-2", 9110)
        retry_ack = await submit(leader, submission)
        job = leader._job_manager.get_job_by_id("job-1")
        return (
            refused_ack,
            left_behind,
            refused_ledger_record,
            refused_payload_kept,
            retry_ack,
            job,
            leader._job_ledger.get_job("job-1"),
            await leader._storage_filesystem.exists(payload_path),
        )

    (
        refused_ack,
        left_behind,
        refused_ledger_record,
        refused_payload_kept,
        retry_ack,
        job,
        retry_ledger_record,
        retry_payload_kept,
    ) = run_scenario(scenario, link, keep_ledger=True)

    assert (refused_ack.accepted, refused_ack.error) == (
        False,
        "Could not quorum-replicate job job-1 before dispatch",
    )
    assert left_behind == (None, None, None, None, [], set())
    assert refused_ledger_record.is_terminal
    assert not refused_payload_kept

    assert retry_ack.accepted
    assert job is not None and job.fencing_token == 1 and len(job.workflows) == 1
    assert not retry_ledger_record.is_terminal
    assert retry_payload_kept
