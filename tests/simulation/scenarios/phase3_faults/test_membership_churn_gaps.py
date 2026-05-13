"""
Phase 3 membership-churn scenarios from ``docs/SCENARIOS.md`` §6.

Large-worker tests use one-core workers with compact port blocks so the
REAL-mode harness can create 50-worker topologies without exhausting the
local port range during sequential scenario runs.
"""

import asyncio

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
    HarnessTimeouts,
    Submission,
    SubmissionPattern,
    WorkloadSpec,
    manager_has_n_workers,
    wait_until,
)


_LARGE_WORKER_COUNT = 50
_COMPACT_WORKER_BLOCK = 32


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


def _long_workload(timeout_seconds: float) -> WorkloadSpec:
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
        expectations=[
            ExpectAllWorkflowsComplete(
                expected_workflow_names=["LongRunningWorkflow"]
            ),
            ExpectCompletionWithin(seconds=timeout_seconds),
        ],
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


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_registration_storm_50_workers() -> None:
    """A manager accepts 50 concurrent worker registrations without dropping them."""
    spec = _single_manager_spec(base_port=43500, workers=0)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="registration_storm_50_workers",
    ) as cluster:
        manager = cluster.managers("local")[0]
        config = manager.instance._config

        async def register(worker_index: int) -> RegistrationResponse:
            tcp_port, udp_port = cluster._ports.reserve_pair()
            registration = _fake_worker_registration(
                node_id=f"storm.worker.{worker_index}",
                host=cluster.spec.host,
                tcp_port=tcp_port,
                udp_port=udp_port,
                datacenter="local",
                cluster_id=config.cluster_id,
                environment_id=config.environment_id,
            )
            response = await manager.instance.worker_register(
                (cluster.spec.host, tcp_port),
                registration.dump(),
                0,
            )
            return RegistrationResponse.load(response)

        responses = await asyncio.gather(
            *(register(worker_index) for worker_index in range(_LARGE_WORKER_COUNT))
        )

        assert all(response.accepted for response in responses)
        assert manager.instance._manager_state.get_worker_count() == _LARGE_WORKER_COUNT


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_graceful_scale_down_50_workers_reassigns_orphans() -> None:
    """Fifty workers leave gracefully while one survivor completes reassigned work."""
    spec = _single_manager_spec(
        base_port=45000,
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
            await driver.wait_until_running(timeout=30.0)

            for victim in victims:
                await cluster.faults.graceful_stop(victim, drain_timeout=1.0)
                await asyncio.sleep(0.2)

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
    """Fifty workers crash at once; the manager drains the worker registry."""
    spec = _single_manager_spec(base_port=47500, workers=_LARGE_WORKER_COUNT)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="mass_crash_50_workers",
    ) as cluster:
        manager = cluster.managers("local")[0]
        workers = cluster.workers("local")

        await cluster.faults.kill_many(workers)

        await wait_until(
            lambda: manager.instance._manager_state.get_worker_count() == 0,
            timeout=120.0,
            poll=0.5,
            description="manager removes every crashed worker",
        )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_slow_worker_churn_one_worker_every_10_seconds() -> None:
    """A time-spaced churn stream repeatedly removes and restores one worker."""
    spec = _single_manager_spec(base_port=49000, workers=2)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="slow_worker_churn_one_worker_every_10_seconds",
    ) as cluster:
        manager = cluster.managers("local")[0]
        victim = cluster.workers("local")[0]

        for churn_index in range(3):
            await cluster.faults.kill(victim)
            await wait_until(
                lambda: manager.instance._manager_state.get_worker_count() <= 1,
                timeout=60.0,
                poll=0.5,
                description=f"slow churn {churn_index}: worker removed",
            )
            await asyncio.sleep(10.0)
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
        base_port=50500,
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

        response_bytes = await manager.instance.worker_register(
            (cluster.spec.host, tcp_port),
            registration.dump(),
            0,
        )
        response = RegistrationResponse.load(response_bytes)

        assert response.accepted is False
        assert response.error is not None
        assert "MAX_WORKERS_PER_MANAGER=1" in response.error
        assert manager.instance._manager_state.get_worker_count() == 1
