"""
E2E (AD-38, live CLI): a job survives its leading manager restarting --
real `hyperscale run gate|manager|worker` processes, each manager with its
own `--data-directory`, and the real `hyperscale run workflow` command
submitting through the gate.

A Kubernetes rolling update or a crash restarts a manager on the same
address and disk within seconds. The manager leading a job came back,
recovered the job from its ledger, and asked its peers about it; any peer
answered from its replica, and the restarted manager took that for the
job being held elsewhere and relinquished it. The replicas, asking the
manager they still recorded as leader, heard it held no such job and
dropped their copies. Nobody led the job: its workers finished, nothing
collected their results, and the gate timed it out.

A gate and three managers form; a worker registers. While a test runs
through the gate, the manager leading its job (the one whose status
answer names it the leader) is stopped and restarted on its directory --
intact, or wiped (its disk lost: it comes back knowing no job, and the
replicas holding the job take it over). The run must still complete and
report the job completed.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import json
import os
import shutil
import signal
import tempfile
import time

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from tests.integration.cli.node_processes import (
    BOOT_TIMEOUT_SECONDS,
    CLI_TEST_AUTH_SECRET,
    HYPERSCALE,
    LOCALHOST,
    NODE_BLOCK,
    RUN_MARKER_ENVAR,
    boot,
    command_environment,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    stop_all,
    worker_block,
)

ENV = Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET)
DATACENTER = "dc-leader-restart"
COHORT_SIZE = 3
WORKER_CORES = 1
CLIENT_BLOCK = 2  # tcp, udp
TEST_NAME = "cli-leader-restart"
# Long enough that the job still runs through its leader's restart.
TEST_DURATION_SECONDS = 20
POLL_INTERVAL_SECONDS = 0.5
# The run's bound: the client's boot, the test's own run time, and the
# leader's restart (bounded like any boot).
RUN_BOUND_SECONDS = 2 * BOOT_TIMEOUT_SECONDS + TEST_DURATION_SECONDS

# An action step (it returns no response) runs once, not for the
# workflow's duration: it sleeps the test's run time so the job is still
# running through its leader's restart.
_TEST_MODULE_SOURCE = '''
import asyncio

from hyperscale.graph import Workflow, step


class LeaderRestartProbe(Workflow):
    vus: int = 1
    duration: str = "{duration}s"

    @step()
    async def probe(self) -> dict[str, str]:
        await asyncio.sleep({duration})
        return {{"status": "ok"}}
'''


@pytest.fixture
def run_marker() -> str:
    return f"cli-leader-restart-{time.monotonic_ns()}"


def _write_test_files(directory: str, client_port: int) -> tuple[str, str]:
    """The test module and the CLI config (its client listens on
    ``client_port``; logs stay in ``directory``); their paths."""
    module_path = os.path.join(directory, "leader_restart_probe.py")
    with open(module_path, "w") as module_file:
        module_file.write(_TEST_MODULE_SOURCE.format(duration=TEST_DURATION_SECONDS))
    logs_directory = os.path.join(directory, "logs")
    os.makedirs(logs_directory)
    config_path = os.path.join(directory, "hyperscale.config.json")
    with open(config_path, "w") as config_file:
        json.dump({"logs_directory": logs_directory, "server_port": client_port, "terminal_mode": "disabled"}, config_file)
    return module_path, config_path


def _manager(start: int, marker: str, cohort_starts: list[int], data_directory: str, gate_start: int):
    return node_at(
        "manager",
        start,
        marker,
        "--datacenter", DATACENTER,
        "--managers", *[f"{LOCALHOST}:{cohort_start}" for cohort_start in cohort_starts],
        "--manager-udp", *[f"{LOCALHOST}:{cohort_start + 1}" for cohort_start in cohort_starts],
        "--gates", f"{LOCALHOST}:{gate_start}",
        "--gate-udp", f"{LOCALHOST}:{gate_start + 1}",
        "--data-directory", data_directory,
    )


async def _wait_for(probe, within: float, description: str):
    """Poll ``probe`` until it returns a value other than None."""
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if (value := await probe()) is not None:
            return value
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"{description} within {within}s")


@pytest.mark.parametrize("disk", ["intact", "wiped"])
async def test_a_job_survives_its_leading_manager_restarting(run_marker: str, disk: str) -> None:
    gate_start, *cohort_starts, worker_start, client_start, observer_start = reserve_port_blocks(
        [NODE_BLOCK] + [NODE_BLOCK] * COHORT_SIZE + [worker_block(WORKER_CORES), CLIENT_BLOCK, CLIENT_BLOCK]
    )
    data_directories = [tempfile.mkdtemp(prefix="hyperscale-leader-restart-") for _ in cohort_starts]
    gate = node_at("gate", gate_start, run_marker)
    managers = [
        _manager(start, run_marker, cohort_starts, directory, gate_start)
        for start, directory in zip(cohort_starts, data_directories)
    ]
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--datacenter", DATACENTER,
        "--workers", str(WORKER_CORES),
        "--managers", managers[0].address,
    )
    nodes = [gate, *managers, worker]
    observer = HyperscaleClient(host=LOCALHOST, port=observer_start, env=ENV)
    run: asyncio.subprocess.Process | None = None
    with tempfile.TemporaryDirectory(prefix="hyperscale-leader-restart-run-") as directory:
        module_path, config_path = _write_test_files(directory, client_start)
        try:
            await boot(gate, *managers)
            for manager in managers:
                assert await manager.wait_for_output("formed: member", within=BOOT_TIMEOUT_SECONDS), (
                    "".join(manager.lines[-30:])
                )
            await boot(worker)
            await observer.start()

            async def datacenter_has_cores() -> bool | None:
                response = await observer.get_datacenters(addr=(LOCALHOST, gate_start))
                return any(
                    info.dc_id == DATACENTER and info.available_cores >= WORKER_CORES for info in response.datacenters
                ) or None

            await _wait_for(datacenter_has_cores, BOOT_TIMEOUT_SECONDS, "the gate never saw the worker's cores")

            run = await asyncio.create_subprocess_exec(
                HYPERSCALE, "run", "workflow", module_path,
                "--config", config_path,
                "--name", TEST_NAME,
                "--quiet",
                "--gates", gate.address,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
                env=command_environment(**{RUN_MARKER_ENVAR: run_marker}),
            )

            async def running_job_id() -> str | None:
                for manager in managers:
                    answer = await observer.ping_manager((LOCALHOST, manager.tcp_port))
                    if answer.active_job_ids:
                        return answer.active_job_ids[0]
                return None

            job_id = await _wait_for(running_job_id, BOOT_TIMEOUT_SECONDS, "no manager held the job")

            async def leader_index() -> int | None:
                for index, manager in enumerate(managers):
                    status = await observer.query_job_status(
                        (LOCALHOST, manager.tcp_port), job_id, timeout=ENV.MANAGER_TCP_TIMEOUT_STANDARD
                    )
                    if status is not None and status.leader_node_id:
                        return index
                return None

            index = await _wait_for(leader_index, BOOT_TIMEOUT_SECONDS, "no manager answered as the job's leader")

            # The leader restarts on its own address and disk, as a rolling
            # update or a crash-and-restart does.
            exited, survivors = await managers[index].stop(signal.SIGTERM, whole_group=False)
            assert exited and survivors == [], f"the leader did not stop: {survivors}"
            if disk == "wiped":
                shutil.rmtree(data_directories[index])
                os.makedirs(data_directories[index])
            restarted = _manager(cohort_starts[index], run_marker, cohort_starts, data_directories[index], gate_start)
            nodes.append(restarted)
            await restarted.start()

            output, _ = await asyncio.wait_for(run.communicate(), timeout=RUN_BOUND_SECONDS)
            report = output.decode(errors="replace")
            assert run.returncode == 0, report
            assert f"{TEST_NAME}: job " in report and " completed" in report, report

            await observer.stop()
            survivors_to_stop = [node for node in nodes if node is not managers[index]]
            await stop_all(survivors_to_stop, signal.SIGTERM, whole_group=False)
        finally:
            if run is not None and run.returncode is None:
                os.killpg(run.pid, signal.SIGKILL)
                await run.wait()
            await observer.stop()
            await kill_remaining(nodes)
            for data_directory in data_directories:
                shutil.rmtree(data_directory, ignore_errors=True)
