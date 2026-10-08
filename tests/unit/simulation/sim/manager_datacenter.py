"""
A datacenter of three real ``ManagerServer`` instances on a
``SimulationLoop``, for scenarios that drive job submissions through the
managers' own handlers.

The one stand-in is the worker: it is registered through the leader's
registration handler and nothing runs at its address, so the managers'
SWIM declares it dead some sixteen virtual seconds after it registers, and
a workflow dispatched to it is never taken.
"""

import asyncio
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TypeVar

import cloudpickle

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import step
from hyperscale.distributed.env.env import Env
from hyperscale.distributed.models import (
    JobAck,
    JobSubmission,
    NodeInfo,
    NodeRole,
    WorkerRegistration,
)
from hyperscale.distributed.nodes.manager.server import ManagerServer
from tests.simulation.harness.sim import SimulationRuntime

from .leader_to_peer_link import LeaderToPeerLink

cloudpickle.register_pickle_by_value(sys.modules[__name__])

HOST = "127.0.0.1"
MANAGER_PORTS = ((9000, 9001), (9002, 9003), (9004, 9005))
MANAGER_TCP_ADDRESSES = frozenset((HOST, tcp_port) for tcp_port, _ in MANAGER_PORTS)
CLIENT_ADDRESS = (HOST, 9500)
DATACENTER = "sim-dc"
# SWIM discovery and the first election settle well inside this.
FORMATION_SECONDS = 40.0
JOB_TIMEOUT_SECONDS = 60.0

ScenarioResult = TypeVar("ScenarioResult")


class Checkout(Workflow):
    vus = 1

    @step()
    async def check_out(self) -> dict:
        return {}


ManagerBuilder = Callable[[int, int], ManagerServer]


def run_scenario(
    scenario: Callable[[list[ManagerServer], ManagerBuilder], Awaitable[ScenarioResult]],
    link: LeaderToPeerLink,
    keep_ledger: bool = False,
    seed: int = 1,
) -> ScenarioResult:
    """Run ``scenario`` against a fresh datacenter, then stop every manager
    still in its list (a scenario that aborts one removes it). The
    scenario also gets the builder of a manager at a ``(tcp, udp)`` port
    pair: a restarted process is a new manager on the old one's ports and
    ledger directory. ``seed`` seeds the run's randomness (election
    jitter among it): each seed is one schedule, replayed exactly."""
    runtime = SimulationRuntime(seed=seed, fault_check=link)

    def build_manager(tcp_port: int, udp_port: int) -> ManagerServer:
        return ManagerServer(
            HOST,
            tcp_port,
            udp_port,
            Env(MERCURY_SYNC_AUTH_SECRET="manager-datacenter-scenario-secret-0123"),
            dc_id=DATACENTER,
            seed_managers=[
                (HOST, peer_tcp_port)
                for peer_tcp_port, _ in MANAGER_PORTS
                if peer_tcp_port != tcp_port
            ],
            manager_udp_peers=[
                (HOST, peer_udp_port)
                for _, peer_udp_port in MANAGER_PORTS
                if peer_udp_port != udp_port
            ],
            wal_data_dir=Path(f"/sim/{HOST}-{tcp_port}/ledger") if keep_ledger else None,
            **runtime.sim_kwargs(),
        )

    try:
        managers = [build_manager(tcp_port, udp_port) for tcp_port, udp_port in MANAGER_PORTS]

        async def run_then_stop() -> ScenarioResult:
            try:
                return await scenario(managers, build_manager)
            finally:
                await asyncio.gather(*(manager.stop() for manager in managers))

        return runtime.run(run_then_stop())
    finally:
        runtime.close()


async def register_worker(leader: ManagerServer, node_id: str, tcp_port: int) -> None:
    registration = WorkerRegistration(
        node=NodeInfo(
            node_id=node_id,
            role=NodeRole.WORKER.value,
            host=HOST,
            port=tcp_port,
            datacenter=DATACENTER,
            udp_port=tcp_port + 1,
        ),
        total_cores=4,
        available_cores=4,
        memory_mb=1024,
        cluster_id=leader._config.cluster_id,
        environment_id=leader._config.environment_id,
    )
    await leader.worker_register((HOST, tcp_port), registration.dump(), 0)


async def form_datacenter(managers: list[ManagerServer], link: LeaderToPeerLink) -> ManagerServer:
    """Start the managers, let them elect, register the stand-in worker
    with the leader, and point ``link`` at the leader."""
    await asyncio.gather(*(manager.start() for manager in managers))
    await asyncio.sleep(FORMATION_SECONDS)
    [leader] = [manager for manager in managers if manager.is_leader()]
    link.leader_address = (HOST, leader._tcp_port)
    await register_worker(leader, "worker-1", 9100)
    return leader


def submission_of(job_id: str, workflow: Workflow, idempotency_key: str | None = None) -> bytes:
    return JobSubmission(
        job_id=job_id,
        workflows=cloudpickle.dumps([("wf-1", [], workflow)]),
        vus=1,
        timeout_seconds=JOB_TIMEOUT_SECONDS,
        idempotency_key=idempotency_key,
    ).dump()


async def submit(manager: ManagerServer, submission: bytes) -> JobAck:
    return JobAck.load(await manager.job_submission(CLIENT_ADDRESS, submission, 0))
