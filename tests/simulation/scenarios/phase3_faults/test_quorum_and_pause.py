"""
Phase 3 multi-failure and pause/resume scenarios.

* ``test_quorum_loss_and_recovery`` — kill 2 of 3 managers, the cluster
  has lost quorum; restart one, the cluster reaches a 2-of-3 state and
  resumes accepting work.

* ``test_manager_pause_and_resume`` — pause a manager (cancel its
  outbound loops without freeing transports), assert peers stop
  hearing from it, resume, assert it rejoins. SIGSTOP/SIGCONT shape;
  fidelity caveat documented in ``FaultMatrix.pause``.

* ``test_cascade_two_managers`` — kill the leader, then immediately
  kill another follower before re-election can complete. Cluster ends
  up with one manager (no quorum). Restart both: assert eventual
  recovery to all-3 visible.
"""

import asyncio

import pytest

from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    HarnessTimeouts,
    ServerHandle,
    dc_has_leader,
    manager_has_n_peers,
    wait_until,
)


def _l2_spec(base_port: int) -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=2, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


def _find_leader(handles: list[ServerHandle]) -> ServerHandle:
    for handle in handles:
        if handle.instance.is_leader():
            return handle
    raise AssertionError("no manager currently reports is_leader() True")


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_quorum_loss_and_recovery() -> None:
    """Kill 2 of 3 managers; restart one; assert 2-of-3 recovery.

    A 3-node cluster needs 2 alive for quorum. After two kills the
    surviving manager cannot win an election (pre-vote needs majority).
    Restarting one of the killed managers brings the cluster back to a
    2-out-of-3 state and election succeeds.
    """
    spec = _l2_spec(base_port=21000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="quorum_loss_and_recovery",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=30.0,
            poll=0.5,
            description="initial leader",
        )

        victims = managers[:2]
        survivor = managers[2]

        await cluster.faults.kill(victims[0])
        await cluster.faults.kill(victims[1])

        # Brief observation window: one manager alone cannot achieve
        # quorum, so it should not claim leadership. We don't assert
        # is_leader() == False on a tight schedule because election
        # state can briefly flicker; instead we restart and verify
        # recovery.
        await asyncio.sleep(2.0)

        # Restart one. Now there are 2 managers — quorum.
        await cluster.faults.restart(victims[0])

        # A leader should emerge from the 2-of-3 set within a few
        # election cycles. Either the survivor or the restarted node
        # may win.
        recovered_set = [survivor, victims[0]]
        await wait_until(
            dc_has_leader(recovered_set),
            timeout=60.0,
            poll=0.5,
            description="leader emerges from 2-of-3 quorum",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="2-of-3 quorum failed to elect"
            ),
        )

        # Restart the second so cleanup verifies the full set comes back.
        await cluster.faults.restart(victims[1])
        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, 2),
                timeout=45.0,
                poll=0.5,
                description=f"{manager.node_id} sees 2 peers after full recovery",
            )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_manager_pause_and_resume() -> None:
    """Pause a follower, observe peers drop it, resume, observe rejoin.

    Pause cancels the SWIM probe loop, leader-election loop, and other
    background tasks on the target node without closing its transports.
    From the cluster's perspective the node is unresponsive — peers
    will stop receiving heartbeats and eventually mark it SUSPECT and
    then DEAD. On resume, the previously-suspended node reconfirms
    itself via incarnation gossip.

    Note: REAL-mode pause/resume is a *soft* approximation — a true
    SIGSTOP would freeze the node's receive path too. Phase 6 SIM mode
    will offer faithful pause semantics.
    """
    spec = _l2_spec(base_port=21100)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="manager_pause_and_resume",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=30.0,
            poll=0.5,
            description="initial leader",
        )

        leader = _find_leader(managers)
        target = next(m for m in managers if m.node_id != leader.node_id)
        peers_of_target = [m for m in managers if m.node_id != target.node_id]

        await cluster.faults.pause(target)
        assert cluster.faults.is_paused(target)

        # Eventually the surviving managers should observe the paused
        # node as having one fewer active peer (the paused node stops
        # responding to probes). Allow a generous suspicion window.
        await wait_until(
            lambda: all(
                len(p.instance._manager_state.get_active_manager_peer_ids()) <= 1
                for p in peers_of_target
            ),
            timeout=60.0,
            poll=0.5,
            description="surviving managers detect paused peer as gone",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="paused manager still appears active to peers"
            ),
        )

        await cluster.faults.resume(target)
        assert not cluster.faults.is_paused(target)

        # After resume, peer counts should converge back to 2 across all
        # three managers. The resumed node bumps its incarnation when it
        # receives stale suspicion gossip, which clears the SUSPECT
        # state on its peers.
        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, 2),
                timeout=60.0,
                poll=0.5,
                description=f"{manager.node_id} sees 2 peers after resume",
            )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_cascade_two_managers() -> None:
    """Kill leader, immediately kill another. Single-manager remainder
    cannot elect. Restart both; assert recovery.

    Tests that the cluster doesn't enter a wedged state when failures
    cascade faster than re-election can complete.
    """
    spec = _l2_spec(base_port=21200)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="cascade_two_managers",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=30.0,
            poll=0.5,
            description="initial leader",
        )

        leader = _find_leader(managers)
        # Kill leader and one follower as quickly as possible.
        non_leaders = [m for m in managers if m.node_id != leader.node_id]
        cascade_victim = non_leaders[0]
        survivor = non_leaders[1]

        await cluster.faults.kill(leader)
        await cluster.faults.kill(cascade_victim)

        # Single survivor cannot satisfy pre-vote majority; observe.
        await asyncio.sleep(3.0)

        # Bring both back; full quorum returns.
        await cluster.faults.restart(leader)
        await cluster.faults.restart(cascade_victim)

        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            poll=0.5,
            description="leader emerges after double restart",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="cluster did not recover after cascade kill"
            ),
        )
        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, 2),
                timeout=45.0,
                poll=0.5,
                description=f"{manager.node_id} reconverges after cascade",
            )
        _ = survivor  # capture so analysers don't flag unused
