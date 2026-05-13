"""
Phase 3 resource-pressure and worker-pool scenarios from ``docs/SCENARIOS.md`` §8.
"""

import psutil
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
    manager_has_n_workers,
    wait_until,
)


def _l1_worker_spec(base_port: int, workers: int = 1) -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "local": DCSpec(managers=1, workers=workers, cores_per_worker=1),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


def _l2_manager_spec(base_port: int) -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=0),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


def _find_leader(managers: list[ServerHandle]) -> ServerHandle:
    for manager in managers:
        if manager.instance.is_leader():
            return manager
    raise AssertionError("no manager currently reports leadership")


def _process_gone_or_zombie(pid: int) -> bool:
    if not psutil.pid_exists(pid):
        return True
    try:
        return psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return True


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_cpu_saturation_marks_worker_overloaded() -> None:
    """Synthetic CPU saturation drives the worker overload detector."""
    spec = _l1_worker_spec(base_port=52000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="cpu_saturation_marks_worker_overloaded",
    ) as cluster:
        worker = cluster.workers("local")[0]

        await cluster.faults.inject_worker_resources(
            worker,
            cpu_percent=99.0,
            memory_percent=0.0,
        )

        assert worker.instance._backpressure_manager.is_overloaded() is True
        assert worker.instance._backpressure_manager.get_overload_state_str() in {
            "overloaded",
            "critical",
        }

        await cluster.faults.clear_worker_resource_injection(worker)


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_memory_pressure_marks_worker_overloaded() -> None:
    """Synthetic memory pressure drives graceful worker overload state."""
    spec = _l1_worker_spec(base_port=53500)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="memory_pressure_marks_worker_overloaded",
    ) as cluster:
        worker = cluster.workers("local")[0]

        await cluster.faults.inject_worker_resources(
            worker,
            cpu_percent=0.0,
            memory_percent=99.0,
        )

        heartbeat = worker.instance._get_heartbeat()
        assert worker.instance._backpressure_manager.is_overloaded() is True
        assert heartbeat.health_overload_state in {"overloaded", "critical"}

        await cluster.faults.clear_worker_resource_injection(worker)


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_worker_subprocess_crash_is_reaped() -> None:
    """A crashed worker-pool child is visible and reaped by the harness."""
    spec = _l1_worker_spec(base_port=55000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_subprocess_crash_is_reaped",
    ) as cluster:
        manager = cluster.managers("local")[0]
        worker = cluster.workers("local")[0]
        pid = await cluster.faults.crash_worker_subprocess(worker)

        await wait_until(
            lambda: _process_gone_or_zombie(pid),
            timeout=10.0,
            poll=0.25,
            description="worker subprocess exits after injected crash",
        )
        assert manager_has_n_workers(manager, 1)() is True


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_worker_subprocess_hang_can_be_resumed() -> None:
    """A suspended worker-pool child models a subprocess hang and can recover."""
    spec = _l1_worker_spec(base_port=56500)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_subprocess_hang_can_be_resumed",
    ) as cluster:
        worker = cluster.workers("local")[0]
        pid = await cluster.faults.hang_worker_subprocess(worker)

        await wait_until(
            lambda: psutil.Process(pid).status() == psutil.STATUS_STOPPED,
            timeout=5.0,
            poll=0.1,
            description="worker subprocess is suspended",
        )

        await cluster.faults.resume_worker_subprocess(pid)
        await wait_until(
            lambda: psutil.Process(pid).status() != psutil.STATUS_STOPPED,
            timeout=5.0,
            poll=0.1,
            description="worker subprocess resumes",
        )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_event_loop_lag_injection_raises_lhm_and_steps_down_leader() -> None:
    """Synthetic event-loop lag drives LHM and leadership handoff."""
    spec = _l2_manager_spec(base_port=58000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="event_loop_lag_injection_raises_lhm_and_steps_down_leader",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            poll=0.5,
            description="initial leader elected",
        )

        leader = _find_leader(managers)
        max_lhm = leader.instance._leader_election.eligibility.max_leader_lhm
        await cluster.faults.inject_event_loop_lag(
            leader,
            critical=True,
            repeats=max_lhm + 1,
        )

        assert leader.instance._local_health.score > 0
        await wait_until(
            lambda: any(
                manager is not leader and manager.instance.is_leader()
                for manager in managers
            ),
            timeout=60.0,
            poll=0.5,
            description="lagged leader steps down",
        )
