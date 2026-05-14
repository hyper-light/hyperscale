"""
Phase 3 membership-churn scenarios from ``docs/SCENARIOS.md`` §6.

Large-worker tests use one-core workers with compact port blocks so the
REAL-mode harness can create 50-worker topologies without exhausting the
local port range during sequential scenario runs.
"""

import asyncio
import time

import pytest

from hyperscale.distributed.models import (
    NodeInfo,
    NodeRole,
    RegistrationResponse,
    WorkerRegistration,
)
from hyperscale.distributed.testing.workflows import LongRunningWorkflow
from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    ExpectAllWorkflowsComplete,
    ExpectCompletionWithin,
    ExpectWorkflowTerminal,
    Expectation,
    HarnessTimeouts,
    Submission,
    SubmissionPattern,
    ServerHandle,
    WorkloadSpec,
    manager_has_n_swim_confirmed_workers,
    manager_has_n_workers,
    wait_until,
)


_LARGE_WORKER_COUNT = 50
_COMPACT_WORKER_BLOCK = 32
_SLOW_CHURN_CYCLES = 30
_SLOW_CHURN_INTERVAL_SECONDS = 10.0
_SCALE_DOWN_WINDOW_SECONDS = 10.0
_LARGE_CLUSTER_RUNNING_TIMEOUT_SECONDS = 120.0


# Each scenario gets a contiguous, non-overlapping port range. A single
# manager + W workers reserves ``2 + W * _COMPACT_WORKER_BLOCK`` ports
# (manager TCP/UDP pair + W worker blocks). The base_ports below are
# spaced with enough headroom that a slow teardown from one scenario
# cannot leak a socket into the next scenario's range — see the
# assertion at the bottom of this module which guards against future
# regressions when worker counts or block sizes change.
_BASE_REGISTRATION_STORM = 43500
_BASE_GRACEFUL_SCALE_DOWN = 45500
_BASE_MASS_CRASH = 47500
_BASE_SLOW_CHURN = 49500
_BASE_BEYOND_MAX = 50500


def _scenario_port_ceiling(base_port: int, workers: int) -> int:
    """Return the first port *past* the range a scenario will reserve.

    A single manager reserves two ports (TCP/UDP pair) and each worker
    reserves ``_COMPACT_WORKER_BLOCK`` ports for its TCP/UDP pair plus
    the derived per-subprocess UDP ports the worker pool spawns. The
    ceiling exists so neighbouring scenarios can detect overlap at
    import time rather than racing each other for the same socket on
    sequential test execution.
    """
    return base_port + 2 + workers * _COMPACT_WORKER_BLOCK


def _single_manager_spec(
    base_port: int,
    workers: int,
    *,
    max_workers_per_manager: int | None = None,
) -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "local": DCSpec(
                managers=1,
                workers=workers,
                cores_per_worker=1,
                worker_port_block_size=_COMPACT_WORKER_BLOCK,
                stabilization_lhm_max_score=(
                    None if workers >= _LARGE_WORKER_COUNT else 0
                ),
            ),
        },
        env=EnvOverrides(
            request_timeout="5s",
            log_level="error",
            max_workers_per_manager=max_workers_per_manager,
        ),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=180.0),
    )


def _long_workload(
    timeout_seconds: float,
    allowed_terminal_statuses: set[str] | None = None,
) -> WorkloadSpec:
    expectations: list[Expectation] = [
        ExpectCompletionWithin(seconds=timeout_seconds),
    ]
    if allowed_terminal_statuses is None:
        expectations.insert(
            0,
            ExpectAllWorkflowsComplete(
                expected_workflow_names=["LongRunningWorkflow"]
            ),
        )
    else:
        expectations.insert(
            0,
            ExpectWorkflowTerminal(
                expected_workflow_names=["LongRunningWorkflow"],
                allowed_statuses=allowed_terminal_statuses,
            ),
        )

    return WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], LongRunningWorkflow)],
                dc_count=1,
                timeout_seconds=timeout_seconds,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=expectations,
    )


def _fake_worker_registration(
    *,
    node_id: str,
    host: str,
    tcp_port: int,
    udp_port: int,
    datacenter: str,
    cluster_id: str,
    environment_id: str,
) -> WorkerRegistration:
    return WorkerRegistration(
        node=NodeInfo(
            node_id=node_id,
            role=NodeRole.WORKER.value,
            host=host,
            port=tcp_port,
            datacenter=datacenter,
            udp_port=udp_port,
        ),
        total_cores=1,
        available_cores=1,
        memory_mb=512,
        available_memory_mb=512,
        cluster_id=cluster_id,
        environment_id=environment_id,
    )


async def _graceful_stop_after_delay(
    cluster: ClusterHarness,
    worker: ServerHandle,
    delay_seconds: float,
) -> None:
    """Start a graceful worker stop at a scheduled offset."""
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    await cluster.faults.graceful_stop(worker, drain_timeout=1.0)


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_registration_storm_50_workers() -> None:
    """A manager accepts 50 concurrent real worker registrations without dropping them."""
    spec = _single_manager_spec(base_port=_BASE_REGISTRATION_STORM, workers=0)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="registration_storm_50_workers",
    ) as cluster:
        manager = cluster.managers("local")[0]

        launch_started = time.monotonic()
        worker_start_tasks = []
        async with asyncio.TaskGroup() as task_group:
            for _worker_index in range(_LARGE_WORKER_COUNT):
                worker_start_tasks.append(
                    task_group.create_task(cluster.add_worker("local"))
                )
            launch_seconds = time.monotonic() - launch_started
        workers = [
            worker_start_task.result()
            for worker_start_task in worker_start_tasks
        ]

        await wait_until(
            manager_has_n_workers(manager, _LARGE_WORKER_COUNT),
            timeout=120.0,
            poll=0.5,
            description="manager accepts every storm worker",
        )
        await wait_until(
            manager_has_n_swim_confirmed_workers(manager, _LARGE_WORKER_COUNT),
            timeout=180.0,
            poll=0.5,
            description="manager confirms every storm worker through SWIM",
        )

        assert launch_seconds < 1.0
        assert len({worker.node_id for worker in workers}) == _LARGE_WORKER_COUNT
        manager_peer_ids = manager.instance._manager_state.get_active_manager_peer_ids()
        worker_ids = set(manager.instance._manager_state.get_worker_ids())
        assert (
            manager.instance._manager_state.get_worker_count()
            == _LARGE_WORKER_COUNT
        )
        for worker in workers:
            worker_swim_node_id = worker.instance._node_id.full
            assert worker.kind.name == "WORKER"
            assert worker_swim_node_id in worker_ids
            assert worker_swim_node_id not in manager_peer_ids


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_graceful_scale_down_50_workers_reassigns_orphans() -> None:
    """Fifty workers leave gracefully while one survivor completes reassigned work."""
    spec = _single_manager_spec(
        base_port=_BASE_GRACEFUL_SCALE_DOWN,
        workers=_LARGE_WORKER_COUNT + 1,
    )
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="graceful_scale_down_50_workers_reassigns_orphans",
    ) as cluster:
        manager = cluster.managers("local")[0]
        workers = cluster.workers("local")
        victims = workers[:_LARGE_WORKER_COUNT]

        async with cluster.workload(_long_workload(90.0)) as driver:
            await driver.submit()
            await driver.wait_until_running(
                timeout=_LARGE_CLUSTER_RUNNING_TIMEOUT_SECONDS
            )

            cluster.set_expected_worker_count("local", 1)
            async with asyncio.TaskGroup() as task_group:
                for victim_index, victim in enumerate(victims):
                    delay_seconds = (
                        victim_index
                        * _SCALE_DOWN_WINDOW_SECONDS
                        / _LARGE_WORKER_COUNT
                    )
                    task_group.create_task(
                        _graceful_stop_after_delay(
                            cluster,
                            victim,
                            delay_seconds,
                        )
                    )

            await wait_until(
                lambda: manager.instance._manager_state.get_worker_count() == 1,
                timeout=90.0,
                poll=0.5,
                description="scale-down leaves one registered worker",
            )
            await driver.wait_for_completion()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_mass_crash_50_workers() -> None:
    """Fifty workers crash at once; the manager drains them and terminates active work."""
    spec = _single_manager_spec(base_port=_BASE_MASS_CRASH, workers=_LARGE_WORKER_COUNT)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="mass_crash_50_workers",
    ) as cluster:
        manager = cluster.managers("local")[0]
        workers = cluster.workers("local")

        async with cluster.workload(
            _long_workload(120.0, {"failed", "cancelled", "timeout"})
        ) as driver:
            await driver.submit()
            await driver.wait_until_running(
                timeout=_LARGE_CLUSTER_RUNNING_TIMEOUT_SECONDS
            )
            cluster.set_expected_worker_count("local", 0)
            await cluster.faults.kill_many(workers)

            await wait_until(
                lambda: manager.instance._manager_state.get_worker_count() == 0,
                timeout=120.0,
                poll=0.5,
                description="manager removes every crashed worker",
            )
            await driver.wait_for_completion()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_slow_worker_churn_one_worker_every_10_seconds() -> None:
    """A time-spaced churn stream repeatedly removes and restores one worker."""
    spec = _single_manager_spec(base_port=_BASE_SLOW_CHURN, workers=2)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="slow_worker_churn_one_worker_every_10_seconds",
    ) as cluster:
        manager = cluster.managers("local")[0]
        victim = cluster.workers("local")[0]

        for churn_index in range(_SLOW_CHURN_CYCLES):
            await cluster.faults.kill(victim)
            await wait_until(
                lambda: manager.instance._manager_state.get_worker_count() <= 1,
                timeout=60.0,
                poll=0.5,
                description=f"slow churn {churn_index}: worker removed",
            )
            await asyncio.sleep(_SLOW_CHURN_INTERVAL_SECONDS)
            await cluster.faults.restart(victim)
            await wait_until(
                manager_has_n_workers(manager, 2),
                timeout=60.0,
                poll=0.5,
                description=f"slow churn {churn_index}: worker restored",
            )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_beyond_max_workers_per_manager_cap_rejected_cleanly() -> None:
    """A manager with a configured worker cap rejects registrations beyond it."""
    spec = _single_manager_spec(
        base_port=_BASE_BEYOND_MAX,
        workers=1,
        max_workers_per_manager=1,
    )
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="beyond_max_workers_per_manager_cap_rejected_cleanly",
    ) as cluster:
        manager = cluster.managers("local")[0]
        config = manager.instance._config
        tcp_port, udp_port = cluster._ports.reserve_pair()
        registration = _fake_worker_registration(
            node_id="over-cap.worker.0",
            host=cluster.spec.host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            datacenter="local",
            cluster_id=config.cluster_id,
            environment_id=config.environment_id,
        )

        response_bytes, _clock = await manager.instance.send_tcp(
            (manager.host, manager.tcp_port),
            "worker_register",
            registration.dump(),
            timeout=5.0,
        )
        response = RegistrationResponse.load(response_bytes)

        assert response.accepted is False
        assert response.error is not None
        assert "MAX_WORKERS_PER_MANAGER=1" in response.error
        assert manager.instance._manager_state.get_worker_count() == 1


# --- Port range overlap guard ---------------------------------------------
# Each scenario reserves a contiguous block of ports; running the suite
# sequentially can leak sockets from one scenario into the next if their
# ranges overlap. Validate at import time so a future increase in
# ``_LARGE_WORKER_COUNT`` or ``_COMPACT_WORKER_BLOCK`` fails loudly
# instead of producing flaky ECONNREFUSED errors at runtime when a
# manager silently fails to bind because the previous scenario still
# owns the port.
_SCENARIO_PORT_RANGES = [
    ("registration_storm",   _BASE_REGISTRATION_STORM,   _LARGE_WORKER_COUNT),
    ("graceful_scale_down",  _BASE_GRACEFUL_SCALE_DOWN,  _LARGE_WORKER_COUNT + 1),
    ("mass_crash",           _BASE_MASS_CRASH,           _LARGE_WORKER_COUNT),
    ("slow_churn",           _BASE_SLOW_CHURN,           2),
    ("beyond_max",           _BASE_BEYOND_MAX,           1),
]
for _i in range(len(_SCENARIO_PORT_RANGES) - 1):
    _name_a, _base_a, _workers_a = _SCENARIO_PORT_RANGES[_i]
    _name_b, _base_b, _workers_b = _SCENARIO_PORT_RANGES[_i + 1]
    _ceiling_a = _scenario_port_ceiling(_base_a, _workers_a)
    assert _ceiling_a <= _base_b, (
        f"port-range overlap: {_name_a} reserves [{_base_a},{_ceiling_a}), "
        f"{_name_b} starts at {_base_b}. Increase {_name_b.upper()}'s "
        "base_port or reduce earlier scenario worker counts."
    )
