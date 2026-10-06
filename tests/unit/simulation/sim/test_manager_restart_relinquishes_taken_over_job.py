"""
A restarted manager leaves a job a peer took over to that peer -- on real
managers, on virtual time.

A manager that restarts finds the jobs it led ACTIVE in its ledger. It
resumed every one it had a persisted submission for, and failed the rest
durably, pushing FAILED to their requestors. But in a datacenter of several
managers, a job whose leader died was taken over by a peer while the leader
was down: the resume ran it a second time under a second leader, and a
failed resume told its requestor it failed while it ran on.

Now each recovered job is asked about across the datacenter first. A peer
holding it -- live, or ended -- has it, and the restarted manager's record
of it is relinquished (a LOCAL ``JobRelinquished``): not resumed, not
failed, its persisted submission discarded. Only a job a quorum of the
datacenter knows nowhere is resumed or failed. A manager that cannot hear
a quorum at boot decides once it can.

* the job's leader dies after admitting it, a survivor takes it over, and
  the leader restarts: it relinquishes the job;
* the same, with the restarted leader cut off from its peers at boot: the
  job waits undecided, and is relinquished once the peers are heard.
"""

import asyncio

import pytest

from hyperscale.distributed.ledger.job_event_applier import JOB_RELINQUISHED_STATUS
from hyperscale.distributed.ledger.job_state import TERMINAL_STATUSES
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

JOB_ID = "job-1"


async def taken_over_by(survivors: list[ManagerServer]) -> ManagerServer:
    """The survivor that leads the job once it is taken over."""
    while not (leaders := [manager for manager in survivors if manager._leases.is_job_leader(JOB_ID)]):
        await asyncio.sleep(1.0)
    [leader] = leaders
    return leader


async def restart_record(restarted: ManagerServer) -> tuple[str | None, bool, bool, bool]:
    """What the restarted manager keeps of the job: its ledger record's
    status, whether its submission is still persisted, whether it holds the
    job as its own, whether it leads it."""
    record = restarted._job_ledger.get_job(JOB_ID)
    held_job = restarted._job_manager.get_job_by_id(JOB_ID)
    return (
        record.status if record is not None else None,
        await restarted._storage_filesystem.exists(
            restarted._config.wal_data_dir / "submissions" / f"{JOB_ID}.bin"
        ),
        held_job is not None and held_job.submission is not None,
        restarted._leases.is_job_leader(JOB_ID),
    )


@pytest.mark.parametrize("cut_off_at_boot", [False, True])
def test_a_restarted_leader_relinquishes_the_job_a_peer_took_over(cut_off_at_boot: bool) -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        accepted_ack = await submit(leader, submission_of(JOB_ID, Checkout()))
        leader_ports = (leader._tcp_port, leader._udp_port)
        leader.abort()
        managers.remove(leader)
        new_leader = await asyncio.wait_for(taken_over_by(managers), timeout=JOB_TIMEOUT_SECONDS)

        restarted = build_manager(*leader_ports)
        managers.append(restarted)
        link.leader_address = (HOST, leader_ports[0])
        link.cut = cut_off_at_boot
        await restarted.start()
        at_boot = await restart_record(restarted)

        # Healed: the next ask, a peer-sync interval after boot, is
        # answered within the short TCP timeout.
        link.cut = False
        await asyncio.sleep(
            restarted._config.peer_job_sync_interval_seconds
            + restarted._config.tcp_timeout_short_seconds
        )
        return (
            accepted_ack,
            at_boot,
            await restart_record(restarted),
            new_leader._leases.is_job_leader(JOB_ID),
            new_leader._job_manager.get_job_by_id(JOB_ID).status,
        )

    accepted_ack, at_boot, settled, still_leads, status_where_led = run_scenario(
        scenario, link, keep_ledger=True
    )

    assert accepted_ack.accepted
    if cut_off_at_boot:
        # No quorum heard: neither resumed nor failed nor relinquished,
        # its submission kept.
        assert at_boot[0] not in TERMINAL_STATUSES
        assert at_boot[1:] == (True, False, False)
    else:
        assert at_boot == (JOB_RELINQUISHED_STATUS, False, False, False)
    assert settled == (JOB_RELINQUISHED_STATUS, False, False, False)
    # The peer that took it over leads it still, the job not failed.
    assert still_leads
    assert status_where_led != "failed"
