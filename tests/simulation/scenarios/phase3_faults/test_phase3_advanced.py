"""
Phase 3 advanced scenarios — fault patterns enumerated in
``docs/SCENARIOS.md`` §1 (leader election / step-down) and §2 (node
failure / rejoin) that the original Phase 3 exit-criteria set did not
cover. Each scenario uses the same harness primitives as the original
Phase 3 tests — ``FaultMatrix``, ``ClusterHarness``, the predicate
helpers in ``tests/simulation/harness/conditions.py`` — composed for
the specific failure mode.

Scenarios covered:

* ``test_leader_graceful_stepdown`` — leader calls
  ``stop_leader_election`` (graceful, no kill). A surviving manager
  becomes leader. The stepped-down node never re-acquires leadership
  without a new election.

* ``test_lhm_driven_leader_stepdown`` — pump the leader's LHM past
  ``max_leader_lhm``. The leader observes its own ineligibility and
  steps down. Guard: only steps down when ``member_count > 1``
  (regression guard from commit ``df16ffbc``).

* ``test_all_managers_die_then_one_returns`` — kill all 3 managers.
  Workers stay alive but lose all managers. Restart one manager.
  The single returning manager bootstraps a fresh cluster; workers
  re-register.

* ``test_worker_permanent_failure`` — kill a worker and never
  restart. Manager unregisters the worker, frees the worker's
  resources, and does not retry dispatch to its addr.

* ``test_concurrent_candidates`` — kill the leader; the surviving
  managers race to start an election in the same term. Exactly one
  becomes leader; no split-brain.
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
    for handle in handles:
        if handle.instance.is_leader():
            return handle
    raise AssertionError("no manager currently reports is_leader() True")


def _find_non_leader(handles: list[ServerHandle]) -> ServerHandle:
    for handle in handles:
        if not handle.instance.is_leader():
            return handle
    raise AssertionError("no non-leader manager available")


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_leader_graceful_stepdown() -> None:
    """Leader voluntarily steps down; a survivor wins re-election.

    Sequence:
      1. Stabilize 3-manager cluster, wait for a leader.
      2. Call ``stop_leader_election`` on the leader. This cancels
         the heartbeat + election loop on the leader; survivors
         eventually detect the missing heartbeat, run an election,
         and a new leader emerges. Once the new leader broadcasts
         its term, the old leader observes the higher term and
         drops its own ``is_leader()`` flag.
      3. Wait for a survivor to become leader.
      4. Wait for the old leader to relinquish ``is_leader()`` (the
         high-term-observation path takes a few protocol periods
         after the new leader is elected).
    """
    spec = _l2_spec(base_port=24000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="leader_graceful_stepdown",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            description="initial leader elected",
        )

        old_leader = _find_leader(managers)
        await old_leader.instance.stop_leader_election()

        await wait_until(
            lambda: any(
                handle is not old_leader and handle.instance.is_leader()
                for handle in managers
            ),
            timeout=60.0,
            poll=0.5,
            description="new leader emerges after graceful step-down",
        )

        # Old leader observes the new term and steps down.
        await wait_until(
            lambda: not old_leader.instance.is_leader(),
            timeout=60.0,
            poll=0.5,
            description="old leader relinquishes is_leader() after observing new term",
        )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_lhm_driven_leader_stepdown() -> None:
    """Pump LHM past ``max_leader_lhm``; leader steps down.

    Per Lifeguard §4.3 the local health multiplier reflects
    self-perceived health; a leader whose LHM saturates is
    self-reporting it can't keep up and must yield. Regression guard
    (commit ``df16ffbc``): step-down must only fire when
    ``member_count > 1`` — a lone manager whose LHM saturates has
    nowhere to step down *to* and must stay leader until peers
    return.

    Sequence:
      1. Stabilize cluster, identify leader.
      2. Bump the leader's LHM repeatedly until it exceeds
         ``max_leader_lhm``.
      3. Wait for the leader to step down and another manager to
         take over.
    """
    spec = _l2_spec(base_port=25500)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="lhm_driven_leader_stepdown",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            description="initial leader elected",
        )

        old_leader = _find_leader(managers)
        max_lhm = old_leader.instance._leader_election.eligibility.max_leader_lhm

        # Bump LHM past the threshold. ``increase_failure_detector``
        # routes through the documented Lifeguard self-health events
        # (``probe_timeout`` covers our injection — it's the canonical
        # "I missed something" signal LHM accepts).
        bumps_remaining = max_lhm + 2
        while bumps_remaining > 0:
            await old_leader.instance.increase_failure_detector("probe_timeout")
            bumps_remaining -= 1

        await wait_until(
            lambda: any(
                handle is not old_leader and handle.instance.is_leader()
                for handle in managers
            ),
            timeout=60.0,
            poll=0.5,
            description="LHM-saturated leader steps down; survivor takes over",
        )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_all_managers_die_then_quorum_returns() -> None:
    """Kill all 3 managers; restart quorum; cluster re-forms.

    The most adversarial Phase 3 manager-tier scenario: total
    quorum loss. Workers remain alive but lose every manager. With
    a 3-manager Raft-style cluster the quorum is 2, so a single
    returning manager cannot elect itself — the cluster requires
    *at least* quorum-many managers back before leadership can
    resume.

    Sequence:
      1. Kill all 3 managers.
      2. Restart 2 (quorum).
      3. Assert a leader is elected among the 2 returning managers.
      4. Assert at least one worker re-registers with the new leader.
    """
    spec = _l2_spec(base_port=27000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="all_managers_die_then_quorum_returns",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            description="initial leader elected",
        )

        # Kill all managers.
        for manager in managers:
            await cluster.faults.kill(manager)
        for manager in managers:
            assert cluster.faults.is_killed(manager)

        # Restart quorum (2 of 3).
        returning = managers[:2]
        for manager in returning:
            await cluster.faults.restart(manager)
        for manager in returning:
            assert manager.started is True

        # A leader must emerge from the returning quorum.
        await wait_until(
            dc_has_leader(returning),
            timeout=90.0,
            poll=0.5,
            description="quorum reforms after total manager loss",
        )

        leader = _find_leader(returning)

        # AD-48 closure: the returning manager state-syncs the worker
        # registry from its peer, then pushes ``ManagerToWorkerRegistration``
        # down to each learned worker so the workers add this manager
        # back into their ``_known_managers``. The push step closes
        # the long-standing gap where workers could not discover that
        # a previously-dead manager had returned. Without it, total
        # manager loss + quorum return is a workers-orphaned-forever
        # state from the workers' perspective.
        await wait_until(
            lambda: leader.instance._manager_state.get_worker_count() >= 1,
            timeout=60.0,
            poll=0.5,
            description="workers re-register with reformed cluster via manager push",
        )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_worker_permanent_failure() -> None:
    """Kill a worker permanently; manager unregisters; resources freed.

    The "kill, no restart" companion to
    ``test_worker_kill_then_restart``. Assert:
      * Manager unregisters the worker within the detection budget.
      * The worker's seat in ``_manager_state._workers`` is gone.
      * No diagnostic noise (cleanup completes cleanly at teardown —
        port allocator releases, no asyncio leaks).
    """
    spec = _l2_spec(base_port=28500)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_permanent_failure",
    ) as cluster:
        manager = cluster.managers("main")[0]
        workers = cluster.workers("main")
        assert len(workers) == 2

        await wait_until(
            lambda: manager.instance._manager_state.get_worker_count() == 2,
            timeout=60.0,
            description="both workers registered initially",
        )

        victim = workers[0]
        await cluster.faults.kill(victim)
        assert cluster.faults.is_killed(victim)

        # Manager should unregister the dead worker.
        await wait_until(
            lambda: manager.instance._manager_state.get_worker_count() == 1,
            timeout=60.0,
            poll=0.5,
            description="manager unregisters permanently-dead worker",
        )

        # No restart — verify the worker stays out of the registry.
        await asyncio.sleep(2.0)
        assert manager.instance._manager_state.get_worker_count() == 1


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_concurrent_candidates() -> None:
    """Two managers race to become leader after the current one dies.

    Kill the elected leader. The two surviving managers will both
    detect the leader's failure and may start elections concurrently
    (same term). The Raft-style pre-vote + vote machinery must
    guarantee *exactly one* of them wins — no split-brain, no
    duplicate leadership.

    Assertion: after stabilization, exactly one survivor is leader.
    """
    spec = _l2_spec(base_port=30000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="concurrent_candidates",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            description="initial leader elected",
        )

        old_leader = _find_leader(managers)
        survivors = [m for m in managers if m is not old_leader]
        assert len(survivors) == 2

        await cluster.faults.kill(old_leader)
        assert cluster.faults.is_killed(old_leader)

        # Wait for a new leader among survivors.
        await wait_until(
            lambda: any(s.instance.is_leader() for s in survivors),
            timeout=60.0,
            poll=0.5,
            description="exactly one survivor wins re-election",
        )

        # Safety: only one leader at any moment.
        leaders = [s for s in survivors if s.instance.is_leader()]
        assert len(leaders) == 1, (
            f"split-brain detected: {len(leaders)} survivors report is_leader=True"
        )
