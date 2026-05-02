"""
Phase 3 worker-fault scenarios.

These exercise the manager-side response to abrupt worker loss and
recovery. The harness already covers the worker-stops-cleanly case via
its lifecycle teardown; these scenarios add the SIGKILL / restart side
of the matrix.

Scenarios:

* ``test_worker_kill_then_restart`` — kill a worker, assert the manager
  detects the loss (worker count drops or worker's SWIM presence
  disappears), then restart and assert re-registration.

* ``test_rapid_worker_churn`` — kill+restart a worker N times in a
  row. Catches resource leaks (port re-reserve, supervisor PID
  tracking, manager-side stale registrations) that only show up under
  repeated lifecycle churn.
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
    manager_has_n_workers,
    wait_until,
)


def _l1_spec(base_port: int, workers: int = 2) -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "local": DCSpec(
                managers=1, workers=workers, cores_per_worker=2
            ),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=45.0),
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_worker_kill_then_restart() -> None:
    """Kill a worker; manager surfaces the loss; restart re-registers.

    Uses an L1-shaped DC with two workers so the manager has someone to
    keep talking to while the killed worker is down. With one worker
    killed, the manager's worker count drops to 1; after restart it
    returns to 2.
    """
    spec = _l1_spec(base_port=20800, workers=2)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_kill_then_restart",
    ) as cluster:
        manager = cluster.managers("local")[0]
        workers = cluster.workers("local")
        assert len(workers) == 2

        victim = workers[0]
        await cluster.faults.kill(victim)
        assert cluster.faults.is_killed(victim)
        assert victim.started is False

        # Manager should drop the killed worker from its registered set.
        # SWIM has to declare the worker DEAD first, then the manager
        # cleans up. Allow a generous budget — failure-detection tuning
        # is a separate concern.
        await wait_until(
            lambda: manager.instance._manager_state.get_worker_count() <= 1,
            timeout=45.0,
            poll=0.5,
            description="manager drops killed worker from registry",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="manager did not unregister killed worker"
            ),
        )

        await cluster.faults.restart(victim)
        assert victim.started is True

        # Restarted worker should re-register.
        await wait_until(
            manager_has_n_workers(manager, 2),
            timeout=30.0,
            poll=0.5,
            description="restarted worker re-registers",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="restarted worker did not re-register"
            ),
        )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_rapid_worker_churn() -> None:
    """Three kill+restart cycles on the same worker; no resource leak.

    Each cycle: kill, wait for the manager to drop the worker, restart,
    wait for re-registration. After the last cycle the cluster's clean
    teardown asserts no port leaks and no leaked async tasks. That
    teardown is the actual leak check — if any kill/restart cycle leaves
    state behind, the supervisor's verifier catches it on harness exit.
    """
    spec = _l1_spec(base_port=20900, workers=2)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="rapid_worker_churn",
    ) as cluster:
        manager = cluster.managers("local")[0]
        victim = cluster.workers("local")[0]

        for cycle in range(3):
            await cluster.faults.kill(victim)
            await wait_until(
                lambda: manager.instance._manager_state.get_worker_count() <= 1,
                timeout=45.0,
                poll=0.5,
                description=f"cycle {cycle}: manager drops worker",
            )

            await cluster.faults.restart(victim)
            await wait_until(
                manager_has_n_workers(manager, 2),
                timeout=30.0,
                poll=0.5,
                description=f"cycle {cycle}: worker re-registers",
            )

            # Brief settle so background tasks fully unwind before the
            # next kill. Without this, mid-rebuild dispatcher state can
            # collide with the next kill in pathological timing.
            await asyncio.sleep(0.5)

    # Clean exit asserts: no port leaks (PortAllocator.verify_all_released),
    # no leaked async tasks (Supervisor._report_async_leaks), no
    # subprocess zombies (Supervisor._final_descendant_sweep).
    assert cluster.supervisor.cleanup_errors == [], (
        f"churn left state behind: {cluster.supervisor.cleanup_errors}"
    )
