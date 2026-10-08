"""
E2E (AD-48, live CLI): a manager restarted after its workers declared it
dead learns them again -- real `hyperscale run manager|worker` processes,
each manager with its own `--data-directory`, as a rolling update restarts
them.

While a manager is down, a worker's SWIM declares it dead and stops talking
to it. The restarted manager knows no worker, so it never talks to the
worker either -- unless its boot request to its peers (`list_workers`)
teaches it theirs, after which it registers with each. That reply is the
model's pickled `dump`; the requester parsed it as a "|"-joined text format,
read every reply as empty, and logged nothing. In a Kubernetes rolling
update the restarted managers sat at "WORKERS 0" for good: a datacenter
that would have had no capacity the moment the one manager its workers
still knew failed.

It then registers with each worker it learned (`manager_register`) so the
worker takes it back -- an endpoint the worker had wired as a reply hook
for a request it never sends, so no request ever reached it.

Three managers form, a worker registers. The third manager is stopped,
the worker's log declares it dead, and it is restarted on its directory:
within the boot bound its dashboard must show the worker healthy, and the
worker must count all three managers healthy again.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import json
import pathlib
import signal
import time

import pytest

from tests.integration.cli.node_processes import (
    BOOT_TIMEOUT_SECONDS,
    LOCALHOST,
    NODE_BLOCK,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    stop_all,
    worker_block,
)

DATACENTER = "dc-late-manager"
COHORT_SIZE = 3
WORKER_CORES = 1
# Wide enough that the dashboard's worker tile is never clipped.
WIDE_TERMINAL = {"COLUMNS": "200", "LINES": "60"}


@pytest.fixture
def run_marker() -> str:
    return f"cli-late-manager-{time.monotonic_ns()}"


def write_ci_config(directory: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    """A .hyperscale.json selecting the "ci" dashboard, logs in a directory
    of their own (out of the frames read below); (config, logs directory)."""
    logs_directory = directory / "logs"
    logs_directory.mkdir()
    config_path = directory / "hyperscale.json"
    config_path.write_text(json.dumps({"logs_directory": str(logs_directory), "terminal_mode": "ci"}))
    return config_path, logs_directory


async def wait_for_log_line(log_path: pathlib.Path, fragment: str, within: float) -> bool:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if log_path.exists() and fragment in log_path.read_text():
            return True
        await asyncio.sleep(0.1)
    return False


async def test_a_manager_restarted_after_its_workers_wrote_it_off_learns_them_again(
    run_marker: str, tmp_path: pathlib.Path
) -> None:
    config_path, logs_directory = write_ci_config(tmp_path)
    *manager_starts, worker_start = reserve_port_blocks([NODE_BLOCK] * COHORT_SIZE + [worker_block(WORKER_CORES)])
    cohort_arguments = (
        "--datacenter", DATACENTER,
        "--managers", *[f"{LOCALHOST}:{start}" for start in manager_starts],
        "--manager-udp", *[f"{LOCALHOST}:{start + 1}" for start in manager_starts],
        "--config", str(config_path),
    )

    def manager_at(start: int):
        return node_at(
            "manager",
            start,
            run_marker,
            *cohort_arguments,
            "--data-directory", str(tmp_path / f"manager-{start}"),
            environment=WIDE_TERMINAL,
            quiet=False,
        )

    managers = [manager_at(start) for start in manager_starts]
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--datacenter", DATACENTER,
        "--workers", str(WORKER_CORES),
        "--managers", managers[0].address,
        "--config", str(config_path),
        # Its CI-safe line reads how many of the managers it knows it
        # counts healthy.
        "--output-mode", "ci-safe",
        environment=WIDE_TERMINAL,
        quiet=False,
    )
    restarted = manager_at(manager_starts[-1])
    nodes = [*managers, worker, restarted]
    try:
        for manager in managers:
            await manager.start()
        await worker.start()
        for manager in managers:
            assert await manager.wait_for_output("1 healthy", within=BOOT_TIMEOUT_SECONDS), (
                f"manager {manager.address} never showed the worker:\n{''.join(manager.lines[-30:])}"
            )

        stopped = managers[-1]
        exited, survivors = await stopped.stop(signal.SIGTERM, whole_group=False)
        assert exited and survivors == [], f"manager {stopped.address} did not stop: {survivors}"
        worker_log = logs_directory / f"worker-{DATACENTER}-{LOCALHOST}-{worker_start}.log"
        assert await wait_for_log_line(
            worker_log, f"[NODE-DEAD] node=('{LOCALHOST}', {manager_starts[-1] + 1})", within=BOOT_TIMEOUT_SECONDS
        ), "the worker never declared the stopped manager dead"

        await restarted.start()
        assert await restarted.wait_for_output("1 healthy", within=BOOT_TIMEOUT_SECONDS), (
            f"the restarted manager never learned the worker:\n{''.join(restarted.lines[-30:])}"
        )
        # And the worker takes the restarted manager back: it registers with
        # the worker (manager_register).
        worker.lines.clear()
        assert await worker.wait_for_output(
            f"known {COHORT_SIZE} healthy {COHORT_SIZE}", within=BOOT_TIMEOUT_SECONDS
        ), f"the worker never took the restarted manager back:\n{''.join(worker.lines[-10:])}"

        await stop_all([*managers[:-1], worker, restarted], signal.SIGINT, whole_group=False)
    finally:
        await kill_remaining(nodes)
