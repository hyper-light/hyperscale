"""
Boot-submission latency scenario -- the picklable manager child the
boot-submission SIM drives: a lone, gateless manager whose log records
the instants that gate its first job's admission, so the test can bound
a boot-time client's submission-to-accept latency by them.

Milestones (values only -- no node ids, no addresses):

* ``("manager-started", t)`` -- after ``start()`` returns
* ``("leader-elected", t)`` -- the instant the manager becomes datacenter
  leader (its become-leader callback, not a sample)
* ``("cluster-formed", t)`` -- the instant its cluster membership forms
* ``("worker-registered", t)`` -- the first 0.5s sample at which its
  registry holds a worker (an upper bound on the registration instant)

``strip_election_retry_hint`` is the scenario's mutation check: the child
answers every "leader unknown" refusal without the election's retry hint
(``LocalLeaderElection.seconds_until_next_decision`` reads 0.0), as the
manager did before refusals carried one, so the client falls back to its
un-hinted back-off ladder. The patch is confined to this child process.
"""

import asyncio
from pathlib import Path

from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.swim.leadership import LocalLeaderElection

from .recovery_faults_demo import _env


def _no_election_retry_hint(election: LocalLeaderElection) -> float:
    """The mutation: no election ever says when it next decides."""
    return 0.0


def boot_submission_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    strip_election_retry_hint=False,
) -> None:
    """Manager child: start a lone gateless ``ManagerServer`` (WAL on) and
    record the milestones that gate its first job's admission."""
    if strip_election_retry_hint:
        LocalLeaderElection.seconds_until_next_decision = _no_election_retry_hint
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id=datacenter_id,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    manager.register_on_become_leader(
        lambda: log.append(("leader-elected", round(context.loop.time(), 6)))
    )

    async def record_cluster_formed() -> None:
        await manager._cluster_membership.wait_formed()
        log.append(("cluster-formed", round(context.loop.time(), 6)))

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

        while manager._manager_state.get_worker_count() < 1:
            await asyncio.sleep(0.5)
        log.append(("worker-registered", round(context.loop.time(), 6)))

    # Watching from before ``start()``: the membership may form while it runs.
    context.loop.create_task(record_cluster_formed())
    context.loop.create_task(run())
