"""
Phase 3 leader-fault scenarios.

These exercise SWIM-tier leader-election behaviour under abrupt
node loss and recovery. Every scenario uses the FaultMatrix to inject
a fault, then asserts the cluster reconverges:

* ``test_leader_kill_then_restart`` — kill the elected leader; assert
  a new leader emerges and the killed node, once restarted, rejoins
  cleanly with a fresh incarnation.

* ``test_follower_kill_then_restart`` — kill a follower manager (does
  not change leadership); assert peer count temporarily drops and
  recovers when the killed node restarts.

Both run on the L2 topology (3 managers + 2 workers in one DC) — the
smallest cluster that has interesting election dynamics. Stabilization
budget intentionally generous (60 s) because these scenarios add a
kill/restart on top of the regular start-up convergence.
"""

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
    manager_is_leader,
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
    """Return the first handle whose instance reports ``is_leader()``.

    Raises ``AssertionError`` if no manager is currently leader. Callers
    must ``wait_until(dc_has_leader(...))`` before calling this.
    """
    for handle in handles:
        if handle.instance.is_leader():
            return handle
    raise AssertionError("no manager currently reports is_leader() True")


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_leader_kill_then_restart() -> None:
    """Kill the elected leader; new leader emerges; killed node rejoins.

    Sequence:
      1. Stabilize 3-manager cluster.
      2. Wait for leader election to complete.
      3. Identify current leader, kill it via ``faults.kill``.
      4. Wait until *some* surviving manager is leader.
      5. Restart the killed manager.
      6. Assert the cluster reconverges to peers=2 from every manager
         (the restarted node has rejoined).
    """
    spec = _l2_spec(base_port=20500)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="leader_kill_then_restart",
    ) as cluster:
        managers = cluster.managers("main")
        assert len(managers) == 3

        # Wait for the first leader to emerge before killing it. The
        # general _stabilize doesn't gate on this — see Phase 2 closure
        # commit message for why.
        await wait_until(
            dc_has_leader(managers),
            timeout=30.0,
            poll=0.5,
            description="initial leader elected",
        )

        leader = _find_leader(managers)
        survivors = [m for m in managers if m.node_id != leader.node_id]

        await cluster.faults.kill(leader)
        assert cluster.faults.is_killed(leader)
        assert leader.started is False

        # New leader from the survivors set within one election cycle
        # plus suspicion timeout. The killed leader's old lease has to
        # expire before survivors will run a real election.
        await wait_until(
            dc_has_leader(survivors),
            timeout=45.0,
            poll=0.5,
            description="new leader after kill",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="no new leader after killing original"
            ),
        )

        # Restart the killed node. It comes back with a fresh NodeId so
        # incarnation comparisons trivially favour the new instance.
        await cluster.faults.restart(leader)
        assert leader.started is True
        assert not cluster.faults.is_killed(leader)

        # Reconvergence: every manager should see 2 SWIM-confirmed peers
        # again (the restarted node is now visible to the other two).
        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, 2),
                timeout=30.0,
                poll=0.5,
                description=f"{manager.node_id} reconfirms 2 peers",
            )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_follower_kill_then_restart() -> None:
    """Kill a non-leader manager; cluster keeps running; restarted node rejoins.

    Unlike the leader-kill scenario, this does not trigger an election.
    Surviving managers continue with the existing leader. The killed
    follower comes back, rejoins via SWIM, and peer counts converge.
    """
    spec = _l2_spec(base_port=20600)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="follower_kill_then_restart",
    ) as cluster:
        managers = cluster.managers("main")

        await wait_until(
            dc_has_leader(managers),
            timeout=30.0,
            poll=0.5,
            description="leader elected",
        )

        leader = _find_leader(managers)
        follower = next(m for m in managers if m.node_id != leader.node_id)

        await cluster.faults.kill(follower)
        assert cluster.faults.is_killed(follower)

        # Leader must still be leader. No election should have fired.
        await wait_until(
            manager_is_leader(leader),
            timeout=10.0,
            poll=0.5,
            description=f"{leader.node_id} retains leadership through follower kill",
        )

        await cluster.faults.restart(follower)
        assert follower.started is True

        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, 2),
                timeout=30.0,
                poll=0.5,
                description=f"{manager.node_id} sees 2 peers after follower rejoin",
            )
