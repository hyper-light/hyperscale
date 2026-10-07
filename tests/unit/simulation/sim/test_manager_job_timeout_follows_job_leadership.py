"""
A job's AD-34 timeout follows the job's own leadership -- on real managers,
on virtual time.

Jobs are led per job, each under its own lease; the cluster leader admits
them and takes over those whose leader died. The timeout followed cluster
leadership instead:

* only the cluster leader checked timeouts, and a manager that lost
  cluster leadership stopped tracking the jobs it still led (telling each
  job's gate it had FAILED) -- a demoted manager's jobs never timed out;
* a takeover installed no timeout strategy -- a taken-over job never timed
  out.

Now each job's leader checks the job's timeout, whatever the cluster
leadership, and a takeover tracks what is left of the job's budget at the
job's leadership fence.

* the cluster leader admits a job and steps down from cluster leadership,
  keeping the job: it times the job out once its budget runs out;
* the cluster leader admits a job and dies: the survivor that takes the
  job over tracks what is left of its budget at the takeover's fence, and
  times the job out once that runs out.

The one stand-in is the worker (nothing runs at its address), so the job
waits for workers throughout and ends only by its timeout.
"""

import asyncio

import pytest

from hyperscale.distributed.ledger.job_event_applier import JOB_TIMED_OUT_STATUS
from hyperscale.distributed.nodes.manager.server import ManagerServer

from .leader_to_peer_link import LeaderToPeerLink
from .manager_datacenter import (
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
# The ledger's hybrid logical clock reads whole milliseconds.
HLC_RESOLUTION_SECONDS = 0.001
# Schedules: each seed is one replayed run of elections and deliveries.
SEEDS = range(1, 6)


def ledger_status(manager: ManagerServer) -> str | None:
    """The job's status in the manager's ledger -- kept after the job's
    teardown, which follows its timeout."""
    record = manager._job_ledger.get_job(JOB_ID)
    return record.status if record is not None else None


async def sleep_until(deadline: float) -> None:
    await asyncio.sleep(max(0.0, deadline - asyncio.get_running_loop().time()))


@pytest.mark.parametrize("seed", SEEDS)
def test_a_manager_that_lost_cluster_leadership_times_out_the_job_it_leads(seed: int) -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], _build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        accepted_ack = await submit(leader, submission_of(JOB_ID, Checkout()))
        submitted_at = asyncio.get_running_loop().time()
        check_interval_seconds = leader._config.job_timeout_check_interval_seconds

        await leader._leader_election._step_down()

        # The budget runs out: another manager leads the cluster, the demoted
        # one still leads the job, which is not timed out yet.
        await sleep_until(submitted_at + JOB_TIMEOUT_SECONDS)
        at_budget = (
            leader.is_leader(),
            any(manager.is_leader() for manager in managers),
            leader._leases.is_job_leader(JOB_ID),
            ledger_status(leader),
        )

        # The job leader's first timeout check past the budget times it out.
        await sleep_until(submitted_at + JOB_TIMEOUT_SECONDS + check_interval_seconds)
        return accepted_ack.accepted, at_budget, leader.is_leader(), ledger_status(leader)

    accepted, at_budget, cluster_leader_at_end, status = run_scenario(scenario, link, keep_ledger=True, seed=seed)

    assert accepted
    is_cluster_leader, cluster_has_leader, leads_job, status_at_budget = at_budget
    assert not is_cluster_leader
    assert cluster_has_leader
    assert leads_job
    assert status_at_budget != JOB_TIMED_OUT_STATUS
    assert not cluster_leader_at_end
    assert status == JOB_TIMED_OUT_STATUS


@pytest.mark.parametrize("seed", SEEDS)
def test_a_taken_over_job_times_out_with_what_is_left_of_its_budget(seed: int) -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], _build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        submission_started_at = leader._clock.monotonic()
        accepted_ack = await submit(leader, submission_of(JOB_ID, Checkout()))
        submitted_at = asyncio.get_running_loop().time()
        accepted_at = leader._clock.monotonic()
        leader_fence = leader._leases.get_fence_token(JOB_ID)

        leader.abort()
        managers.remove(leader)
        claimant = await asyncio.wait_for(taken_over_by(managers), timeout=JOB_TIMEOUT_SECONDS)
        tracking = claimant._job_manager.get_job_by_id(JOB_ID).timeout_tracking
        tracked = (
            None
            if tracking is None
            else (
                tracking.started_at - submission_started_at,
                tracking.started_at - accepted_at,
                tracking.timeout_seconds,
                tracking.timeout_fence_token,
            )
        )
        claimant_fence = claimant._leases.get_fence_token(JOB_ID)

        check_interval_seconds = claimant._config.job_timeout_check_interval_seconds
        await sleep_until(submitted_at + JOB_TIMEOUT_SECONDS + check_interval_seconds)
        return (
            accepted_ack.accepted,
            tracked,
            leader_fence,
            claimant_fence,
            ledger_status(claimant),
        )

    accepted, tracked, leader_fence, claimant_fence, status = run_scenario(scenario, link, keep_ledger=True, seed=seed)

    assert accepted
    assert tracked is not None
    since_submission_seconds, since_acceptance_seconds, remaining_budget_seconds, timeout_fence = tracked
    assert 0.0 < since_acceptance_seconds < JOB_TIMEOUT_SECONDS
    # What was left when it was taken over: the budget less everything since
    # the job's record was created -- during its submission -- within the
    # millisecond the ledger's HLC reads.
    assert (
        JOB_TIMEOUT_SECONDS - since_submission_seconds - HLC_RESOLUTION_SECONDS
        <= remaining_budget_seconds
        <= JOB_TIMEOUT_SECONDS - since_acceptance_seconds + HLC_RESOLUTION_SECONDS
    )
    # Resumed at the takeover's leadership fence, above the first leader's.
    assert timeout_fence == claimant_fence > leader_fence
    assert status == JOB_TIMED_OUT_STATUS
