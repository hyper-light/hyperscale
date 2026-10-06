"""
E2E: `hyperscale run workflow` against a running cluster -- real
`hyperscale run manager|worker|gate` processes, a real `hyperscale join`,
and the real `hyperscale run workflow` command driving its ClusterRunner.

* GATELESS: the CLI runs a test straight on one datacenter's managers
  (`--managers`); it exits 0 and reports the job completed.
* THROUGH A GATE: the same test through a gate the manager joined
  (`--gates`); it exits 0 and reports the job completed.
* OPERATOR STOP: SIGINT while the test runs cancels the job on the
  cluster -- a job left running would keep loading its target unseen --
  and the CLI exits 130; the manager's active jobs drain.

Each scenario starts the CLI once the cluster reports the worker's cores
(a manager or gate rejects work until a worker registered capacity).
Bounds come from the configuration under test: a run is bounded by the
CLI's client boot (bounded like a node's) plus the test's own duration;
a stopped run's job leaves the manager within its cancelled-workflow
timeout (the stuck-CANCELLING sweep's bound).

Run from the repo root (the command is invoked from `.venv/bin`).
"""

import asyncio
import json
import os
import signal
import tempfile
import time

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import DatacenterInfo, ManagerPingResponse
from hyperscale.distributed.nodes.client import HyperscaleClient
from tests.integration.cli.node_processes import (
    BOOT_TIMEOUT_SECONDS,
    HYPERSCALE,
    LOCALHOST,
    NODE_BLOCK,
    RUN_MARKER_ENVAR,
    SHUTDOWN_TIMEOUT_SECONDS,
    boot,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    run_join,
    stop_all,
    worker_block,
)

ENV = Env()
WORKER_CORES = 1
CLIENT_BLOCK = 2  # tcp, udp
DATACENTER = "default"
TEST_NAME = "cli-cluster-run"

# The short test's own run time; its bound adds the client's boot.
SHORT_TEST_DURATION_SECONDS = 2
RUN_BOUND_SECONDS = BOOT_TIMEOUT_SECONDS + SHORT_TEST_DURATION_SECONDS

# A test that can only end by the operator's stop: it runs far past every
# bound the stop scenario waits on.
STOPPED_TEST_DURATION_SECONDS = int(
    BOOT_TIMEOUT_SECONDS + SHUTDOWN_TIMEOUT_SECONDS + ENV.CANCELLED_WORKFLOW_TIMEOUT
) * 2

POLL_INTERVAL_SECONDS = 0.5

# A shell exits 128 + the signal's number when a signal ends a process; the
# CLI reports an operator's stop the same way.
OPERATOR_STOP_EXIT_CODE = 128 + signal.SIGINT.value

_TEST_MODULE_SOURCE = '''
from hyperscale.graph import Workflow, step


class ClusterRunProbe(Workflow):
    vus: int = 1
    duration: str = "{duration}s"

    @step()
    async def probe(self) -> dict[str, str]:
        return {{"status": "ok"}}
'''


@pytest.fixture
def run_marker() -> str:
    return f"cli-cluster-run-{time.monotonic_ns()}"


def _write_test_files(directory: str, duration_seconds: int, client_port: int) -> tuple[str, str]:
    """The test module and the CLI config (its client listens on
    ``client_port``; logs stay in ``directory``). Returns their paths."""
    module_path = os.path.join(directory, f"cluster_run_probe_{duration_seconds}.py")
    with open(module_path, "w") as module_file:
        module_file.write(_TEST_MODULE_SOURCE.format(duration=duration_seconds))

    logs_directory = os.path.join(directory, "logs")
    os.makedirs(logs_directory)
    config_path = os.path.join(directory, "hyperscale.config.json")
    with open(config_path, "w") as config_file:
        json.dump(
            {
                "logs_directory": logs_directory,
                "server_port": client_port,
                "terminal_mode": "disabled",
            },
            config_file,
        )
    return module_path, config_path


async def _start_cli_run(
    module_path: str,
    config_path: str,
    marker: str,
    *cluster_flags: str,
) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        HYPERSCALE,
        "run",
        "workflow",
        module_path,
        "--config",
        config_path,
        "--name",
        TEST_NAME,
        "--quiet",
        *cluster_flags,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
        env={**os.environ, RUN_MARKER_ENVAR: marker},
    )


async def _wait_for_manager(
    client: HyperscaleClient,
    manager_port: int,
    matches,
    within: float,
) -> ManagerPingResponse:
    """Ping the manager until its answer satisfies ``matches``."""
    deadline = time.monotonic() + within
    last_seen: ManagerPingResponse | None = None
    while time.monotonic() < deadline:
        last_seen = await client.ping_manager((LOCALHOST, manager_port))
        if matches(last_seen):
            return last_seen
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"manager never matched within {within}s; last answer: {last_seen}")


async def _wait_for_datacenter(
    client: HyperscaleClient,
    gate_port: int,
    matches,
    within: float,
) -> DatacenterInfo:
    """Poll the gate's datacenter list until DATACENTER satisfies ``matches``."""
    deadline = time.monotonic() + within
    last_seen: DatacenterInfo | None = None
    while time.monotonic() < deadline:
        response = await client.get_datacenters(addr=(LOCALHOST, gate_port))
        for info in response.datacenters:
            if info.dc_id == DATACENTER:
                last_seen = info
                if matches(info):
                    return info
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"datacenter never matched within {within}s; last seen: {last_seen}")


async def test_cli_runs_a_test_on_one_datacenters_managers(run_marker: str) -> None:
    worker_start, manager_start, client_start, observer_start = reserve_port_blocks(
        [worker_block(WORKER_CORES), NODE_BLOCK, CLIENT_BLOCK, CLIENT_BLOCK]
    )
    manager = node_at("manager", manager_start, run_marker)
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--workers", str(WORKER_CORES),
        "--managers", manager.address,
    )
    nodes = [manager, worker]
    observer = HyperscaleClient(host=LOCALHOST, port=observer_start, env=Env())
    with tempfile.TemporaryDirectory(prefix="hyperscale-cli-run-") as directory:
        module_path, config_path = _write_test_files(
            directory, SHORT_TEST_DURATION_SECONDS, client_start
        )
        try:
            await boot(manager, worker)
            await observer.start()
            await _wait_for_manager(
                observer,
                manager.tcp_port,
                lambda answer: answer.available_cores >= WORKER_CORES,
                within=BOOT_TIMEOUT_SECONDS,
            )

            run = await _start_cli_run(
                module_path, config_path, run_marker, "--managers", manager.address
            )
            output, _ = await asyncio.wait_for(run.communicate(), timeout=RUN_BOUND_SECONDS)
            report = output.decode(errors="replace")

            assert run.returncode == 0, report
            assert f"{TEST_NAME}: job " in report and " completed" in report, report

            await observer.stop()
            await stop_all(nodes, signal.SIGTERM, whole_group=False)
        finally:
            await observer.stop()
            await kill_remaining(nodes)


async def test_cli_runs_a_test_through_a_gate(run_marker: str) -> None:
    worker_start, manager_start, gate_start, join_start, client_start, observer_start = (
        reserve_port_blocks(
            [
                worker_block(WORKER_CORES),
                NODE_BLOCK,
                NODE_BLOCK,
                CLIENT_BLOCK,
                CLIENT_BLOCK,
                CLIENT_BLOCK,
            ]
        )
    )
    manager = node_at("manager", manager_start, run_marker)
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--workers", str(WORKER_CORES),
        "--managers", manager.address,
    )
    gate = node_at("gate", gate_start, run_marker)
    nodes = [manager, worker, gate]
    observer = HyperscaleClient(host=LOCALHOST, port=observer_start, env=Env())
    with tempfile.TemporaryDirectory(prefix="hyperscale-cli-run-") as directory:
        module_path, config_path = _write_test_files(
            directory, SHORT_TEST_DURATION_SECONDS, client_start
        )
        try:
            await boot(manager, worker, gate)
            returncode, output = await run_join(manager.address, gate.address, join_start)
            assert returncode == 0, output
            await observer.start()
            await _wait_for_datacenter(
                observer,
                gate.tcp_port,
                lambda info: info.available_cores >= WORKER_CORES,
                within=BOOT_TIMEOUT_SECONDS,
            )

            run = await _start_cli_run(
                module_path, config_path, run_marker, "--gates", gate.address
            )
            output, _ = await asyncio.wait_for(run.communicate(), timeout=RUN_BOUND_SECONDS)
            report = output.decode(errors="replace")

            assert run.returncode == 0, report
            assert f"{TEST_NAME}: job " in report and " completed" in report, report

            await observer.stop()
            await stop_all(nodes, signal.SIGTERM, whole_group=False)
        finally:
            await observer.stop()
            await kill_remaining(nodes)


async def test_operator_stop_cancels_the_job_on_the_cluster(run_marker: str) -> None:
    worker_start, manager_start, client_start, observer_start = reserve_port_blocks(
        [worker_block(WORKER_CORES), NODE_BLOCK, CLIENT_BLOCK, CLIENT_BLOCK]
    )
    manager = node_at("manager", manager_start, run_marker)
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--workers", str(WORKER_CORES),
        "--managers", manager.address,
    )
    nodes = [manager, worker]
    observer = HyperscaleClient(host=LOCALHOST, port=observer_start, env=Env())
    with tempfile.TemporaryDirectory(prefix="hyperscale-cli-run-") as directory:
        module_path, config_path = _write_test_files(
            directory, STOPPED_TEST_DURATION_SECONDS, client_start
        )
        try:
            await boot(manager, worker)
            await observer.start()
            await _wait_for_manager(
                observer,
                manager.tcp_port,
                lambda answer: answer.available_cores >= WORKER_CORES,
                within=BOOT_TIMEOUT_SECONDS,
            )

            run = await _start_cli_run(
                module_path, config_path, run_marker, "--managers", manager.address
            )
            running = await _wait_for_manager(
                observer,
                manager.tcp_port,
                lambda answer: answer.active_workflow_count > 0,
                within=RUN_BOUND_SECONDS,
            )
            (job_id,) = running.active_job_ids

            run.send_signal(signal.SIGINT)
            output, _ = await asyncio.wait_for(run.communicate(), timeout=SHUTDOWN_TIMEOUT_SECONDS)
            report = output.decode(errors="replace")
            assert run.returncode == OPERATOR_STOP_EXIT_CODE, report

            drained = await _wait_for_manager(
                observer,
                manager.tcp_port,
                lambda answer: job_id not in answer.active_job_ids,
                within=ENV.CANCELLED_WORKFLOW_TIMEOUT,
            )
            assert drained.active_workflow_count == 0, drained

            await observer.stop()
            await stop_all(nodes, signal.SIGTERM, whole_group=False)
        finally:
            await observer.stop()
            await kill_remaining(nodes)
