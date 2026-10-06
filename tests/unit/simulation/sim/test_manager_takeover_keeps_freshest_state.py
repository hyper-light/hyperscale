"""
A takeover keeps the freshest view of the job -- on real managers, on
virtual time.

Taking a job over, the cluster leader first pulls every peer's state, and
applied each peer's copy of the job as if it were the job leader's own
word -- mirrored, the last applied winning. A peer that missed the leader's
last syncs carried an older view, and the claimant's fresher one regressed
to it: a running job read as queued, and a workflow the leader had seen
finish read as running -- for the new leader to run again. A follower now
mirrors only the job's leader; a peer's copy is evidence of how far the
job got, merged forward.

The leader admits the job; one follower is cut from it right after the
admission's replication, so the leader's next periodic sync marks the job
running on the other alone. The leader dies and a survivor takes the job
over: a fresh claimant's pre-claim pull meets the stale copy and keeps the
job running; a stale claimant's meets the fresh copy and merges forward to
running. The run is made with each follower cut in turn, over several
seeds -- which survivor the election favors follows the schedule -- and
must take both directions: a fresh claimant (the direction the regression
took) and a stale one.
"""

import asyncio

from hyperscale.distributed.models import JobStatus
from hyperscale.distributed.nodes.manager.server import ManagerServer

from .leader_to_peer_link import LeaderToPeerLink
from .manager_datacenter import (
    HOST,
    JOB_TIMEOUT_SECONDS,
    MANAGER_TCP_ADDRESSES,
    Checkout,
    ManagerBuilder,
    form_datacenter,
    run_scenario,
    submission_of,
    submit,
)
from .test_manager_restart_relinquishes_taken_over_job import taken_over_by

JOB_ID = "job-1"


def job_status(manager: ManagerServer) -> str | None:
    job = manager._job_manager.get_job_by_id(JOB_ID)
    return job.status if job is not None else None


def takeover_after_one_follower_missed_a_sync(
    cut_lower_port_follower: bool,
    seed: int = 1,
) -> tuple[bool, tuple[str | None, str | None], bool, str | None]:
    """The job's admission, a sync one follower misses, the leader's death
    and the takeover: (accepted, the followers' statuses before the death
    as (stale, fresh), whether the fresh follower claimed, the claimant's
    status after)."""
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        stale_follower, fresh_follower = sorted(
            (manager for manager in managers if manager is not leader),
            key=lambda manager: manager._tcp_port,
            reverse=not cut_lower_port_follower,
        )
        register_job_workflows = leader._register_job_workflows

        async def register_then_cut_stale_follower(*args, **kwargs) -> None:
            await register_job_workflows(*args, **kwargs)
            link.cut_destinations = frozenset({(HOST, stale_follower._tcp_port)})

        leader._register_job_workflows = register_then_cut_stale_follower
        accepted_ack = await submit(leader, submission_of(JOB_ID, Checkout()))
        await asyncio.sleep(leader._config.peer_job_sync_interval_seconds)
        before_death = (job_status(stale_follower), job_status(fresh_follower))

        leader.abort()
        managers.remove(leader)
        claimant = await asyncio.wait_for(taken_over_by(managers), timeout=JOB_TIMEOUT_SECONDS)
        return accepted_ack.accepted, before_death, claimant is fresh_follower, job_status(claimant)

    return run_scenario(scenario, link, seed=seed)


# Schedules: each seed is one replayed run of elections and deliveries.
SEEDS = range(1, 5)


def test_a_takeover_does_not_regress_the_job_to_a_stale_peers_copy() -> None:
    runs = [
        takeover_after_one_follower_missed_a_sync(cut_lower_port_follower, seed)
        for seed in SEEDS
        for cut_lower_port_follower in (False, True)
    ]

    for accepted, before_death, _fresh_follower_claimed, status_after_takeover in runs:
        assert accepted
        assert before_death == (JobStatus.QUEUED.value, JobStatus.RUNNING.value)
        assert status_after_takeover == JobStatus.RUNNING.value
    # Both directions were taken: a fresh claimant whose pull met the stale
    # copy (the direction the regression took), and a stale claimant whose
    # pull met the fresh one.
    assert {fresh_follower_claimed for _, _, fresh_follower_claimed, _ in runs} == {True, False}
