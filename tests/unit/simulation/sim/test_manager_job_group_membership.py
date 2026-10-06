"""
A job's Raft group changes its members only through its log -- on real
managers, on virtual time (AD-52 slice B).

Every member created a job's group with the voters it happened to know of,
and edited them locally as managers came and went: members that knew
different managers counted different quorums -- two majorities that need
not overlap -- and a manager restarted mid-job (a new incarnation: its id
embeds its start time) voted as soon as any member added it, alongside
the incarnation it replaced.

Now the job's creator decides the group's voters and hands them to every
member that joins the group; the group's Raft leader then moves the
membership through the log, toward the managers live now: a departed
incarnation stops voting, a new one follows as a learner, and is promoted
once it holds every committed entry.

Three real ``ManagerServer`` instances form a datacenter on a
``SimulationLoop``; the one stand-in is the worker (the job waits for it
throughout, so its group lives the whole run).

* every member holds the job's group with the voters its leader decided;
* a follower restarted mid-job rejoins as a voter -- its old incarnation
  gone from the group -- through the group's log alone.
"""

import asyncio

from hyperscale.distributed.nodes.manager.server import ManagerServer

from .leader_to_peer_link import LeaderToPeerLink
from .manager_datacenter import (
    HOST,
    MANAGER_TCP_ADDRESSES,
    Checkout,
    ManagerBuilder,
    form_datacenter,
    run_scenario,
    submission_of,
    submit,
)

JOB_ID = "job-1"
# The restarted manager registers with its peers, learns the job by their
# next sync, and is promoted a few group heartbeats after it catches up.
REJOIN_SECONDS = 30.0


def group_view(manager: ManagerServer) -> tuple[frozenset[str], frozenset[str], frozenset[str], bool] | None:
    """The job's group on ``manager``: its agreed initial voters, and its
    current voters, learners and whether it is mid-change."""
    if (node := manager._raft.consensus.get_node(JOB_ID)) is None:
        return None
    configuration = node.configuration
    return (
        node.initial_voters,
        configuration.voters,
        configuration.learners,
        configuration.is_joint,
    )


def test_every_member_joins_the_job_group_with_its_leaders_voters() -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], _build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        ack = await submit(leader, submission_of(JOB_ID, Checkout()))
        await asyncio.sleep(leader._config.peer_job_sync_interval_seconds)
        return ack, [manager._node_id.full for manager in managers], [
            group_view(manager) for manager in managers
        ]

    ack, manager_ids, views = run_scenario(scenario, link)

    assert ack.accepted
    assert views == [(frozenset(manager_ids),) * 2 + (frozenset(), False)] * len(manager_ids)


def test_a_manager_restarted_mid_job_rejoins_the_group_through_its_log() -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], build_manager: ManagerBuilder):
        leader = await form_datacenter(managers, link)
        ack = await submit(leader, submission_of(JOB_ID, Checkout()))
        await asyncio.sleep(leader._config.peer_job_sync_interval_seconds)

        [follower, *_] = [manager for manager in managers if manager is not leader]
        replaced_id = follower._node_id.full
        follower_ports = (follower._tcp_port, follower._udp_port)
        follower.abort()
        managers.remove(follower)

        restarted = build_manager(*follower_ports)
        managers.append(restarted)
        await restarted.start()
        await asyncio.sleep(REJOIN_SECONDS)

        live_ids = frozenset(manager._node_id.full for manager in managers)
        return ack, replaced_id, live_ids, [group_view(manager) for manager in managers]

    ack, replaced_id, live_ids, views = run_scenario(scenario, link)

    assert ack.accepted
    for initial_voters, voters, learners, is_joint in views:
        # The group keeps the voters it was created with as its origin...
        assert replaced_id in initial_voters
        # ...and its log moved it to the managers live now.
        assert (voters, learners, is_joint) == (live_ids, frozenset(), False)
