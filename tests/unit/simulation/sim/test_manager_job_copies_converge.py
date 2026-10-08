"""
A copy of a job led elsewhere converges on the job's fate -- on real
managers, on virtual time.

A job's leader tells its peers a job is over with one terminal sync. Lost
-- to the very partition that refused the job's submission, or to the
leader dying -- the peers kept the job as it last heard it, forever: never
swept (only ended jobs are), its consensus group heartbeating, and a
takeover candidate. When the leader died, the cluster leader took such a
copy over: a job announced and refused, with nothing to run, "completed";
a job that had ended ran again, its ending committed in the ledger but
applied by its dead leader alone -- a new consensus leader appended
nothing of its own, and an entry of an earlier term commits only behind
one (Raft 5.4.2, 8).

Now a copy that heard nothing for a whole sweep is asked about -- of its
leader, or the datacenter leader when its leader is dead -- and dropped
(no such job, or one never admitted) or ended; a takeover waits for the
job's consensus group to settle, and settles a job that ended or was
never admitted instead of taking it over; members refuse a claim on a job
they know ended.

* a refused job's announced copies are dropped once its live leader is
  asked, and while undecided a follower tells a resubmission to retry;
* a refused job whose leader died is dropped, not taken over;
* a job that ended unannounced is settled ended, not taken over, when its
  leader dies.
"""

import asyncio

import pytest

from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.protocol.transient_errors import is_transient_rejection

from .leader_to_peer_link import LeaderToPeerLink
from .manager_datacenter import (
    ManagerBuilder,
    JOB_TIMEOUT_SECONDS,
    MANAGER_TCP_ADDRESSES,
    Checkout,
    form_datacenter,
    run_scenario,
    submission_of,
    submit,
)

JOB_ID = "job-1"


def held_by(manager: ManagerServer) -> tuple[str | None, bool, bool]:
    """What ``manager`` holds of the job: its status, its lease, its
    consensus group."""
    job = manager._job_manager.get_job_by_id(JOB_ID)
    return (
        job.status if job is not None else None,
        manager._leases.get_job_leader(JOB_ID) is not None,
        manager._raft.consensus.get_node(JOB_ID) is not None,
    )


def cut_after_announcing(leader: ManagerServer, link: LeaderToPeerLink) -> None:
    """The partition starts once the leader announced the job: its peers
    hold copies, and nothing after -- replication, the terminal sync --
    reaches them."""
    announce = leader._broadcast_job_leadership

    async def announce_then_partition(*args, **kwargs) -> None:
        await announce(*args, **kwargs)
        link.cut = True

    leader._broadcast_job_leadership = announce_then_partition


@pytest.mark.parametrize("keep_ledger", [False, True])
def test_a_refused_jobs_copies_are_dropped_once_its_leader_is_asked(keep_ledger: bool) -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        followers = [manager for manager in managers if manager is not leader]
        cut_after_announcing(leader, link)
        refused_ack = await submit(leader, submission_of(JOB_ID, Checkout()))
        announced_copies = [held_by(follower) for follower in followers]
        undecided_ack = await submit(followers[0], submission_of(JOB_ID, Checkout()))

        # Healed: a copy that heard nothing for a whole sweep is asked
        # about at the next -- two sweep intervals after the announcement.
        link.cut = False
        await asyncio.sleep(2 * leader._config.job_cleanup_interval_seconds)
        return refused_ack, announced_copies, undecided_ack, [held_by(manager) for manager in managers]

    refused_ack, announced_copies, undecided_ack, held_after = run_scenario(
        scenario, link, keep_ledger=keep_ledger
    )

    assert not refused_ack.accepted
    assert announced_copies == [("queued", True, True), ("queued", True, True)]
    assert (undecided_ack.accepted, undecided_ack.error) == (False, "submission in progress, retry")
    assert is_transient_rejection(undecided_ack.error)
    assert held_after == [(None, False, False)] * 3


@pytest.mark.parametrize("keep_ledger", [False, True])
def test_a_refused_job_whose_leader_died_is_dropped_not_taken_over(keep_ledger: bool) -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        cut_after_announcing(leader, link)
        refused_ack = await submit(leader, submission_of(JOB_ID, Checkout()))
        leader.abort()
        managers.remove(leader)

        # Within a sweep the leader is declared dead, a survivor leads the
        # datacenter and settles the job; a copy silent for a whole sweep
        # is asked about at the next -- three sweep intervals in all.
        await asyncio.sleep(3 * leader._config.job_cleanup_interval_seconds)
        return refused_ack, [held_by(manager) for manager in managers]

    refused_ack, held_after = run_scenario(scenario, link, keep_ledger=keep_ledger)

    assert not refused_ack.accepted
    assert held_after == [(None, False, False)] * 2


def test_a_job_that_ended_unannounced_is_settled_not_taken_over_when_its_leader_dies() -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        dead_leader_id = leader._node_id.full

        # Its workflow is never taken (the stand-in worker runs nothing),
        # so the job times out. The leader records the end in the ledger;
        # the partition starts before its terminal sync goes out.
        tear_down = leader._cleanup_job_state
        ended = asyncio.Event()

        async def partition_then_tear_down(job_id: str) -> None:
            if job_id == JOB_ID:
                link.cut = True
                ended.set()
            await tear_down(job_id)

        leader._cleanup_job_state = partition_then_tear_down
        accepted_ack = await submit(leader, submission_of(JOB_ID, Checkout()))
        await asyncio.wait_for(ended.wait(), timeout=2 * JOB_TIMEOUT_SECONDS)
        leader.abort()
        managers.remove(leader)

        await asyncio.sleep(3 * leader._config.job_cleanup_interval_seconds)
        return accepted_ack, dead_leader_id, [
            (
                held_by(manager),
                manager._leases.get_job_leader(JOB_ID),
                [key for key in manager._workflow_dispatcher._pending if key.startswith(f"{JOB_ID}:")],
            )
            for manager in managers
        ]

    accepted_ack, dead_leader_id, survivors = run_scenario(scenario, link, keep_ledger=True)

    assert accepted_ack.accepted
    # Ended on both survivors, its group gone, still leased to the dead
    # leader -- nobody took it over -- and nothing of it queued to run.
    assert survivors == [(("timeout", True, False), dead_leader_id, [])] * 2
