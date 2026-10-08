"""
E2E: a manager cohort formed from seed locators (AD-52 section 2), real
processes.

Three `hyperscale run manager` processes are each given the cohort as one
`file://` locator per flag -- a listing the operator writes once -- and the
cohort size they agree on. Asserts:

1. All three resolve the same cohort and form one membership.
2. A manager told a different cohort size refuses to boot, saying why,
   instead of founding a cluster of the members it happened to resolve.

Bounds come from configuration: boot by --boot-timeout.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import pathlib
import tempfile
import time

import pytest

from tests.integration.cli.node_processes import (
    BOOT_TIMEOUT_SECONDS,
    LOCALHOST,
    NODE_BLOCK,
    boot,
    kill_remaining,
    node_at,
    reserve_port_blocks,
)

DATACENTER = "dc-seed-locators"
COHORT_SIZE = 3


@pytest.fixture
def run_marker() -> str:
    return f"cli-seed-locators-{time.monotonic_ns()}"


async def test_a_cohort_forms_from_file_locators(run_marker: str) -> None:
    manager_starts = reserve_port_blocks([NODE_BLOCK] * COHORT_SIZE)
    with tempfile.TemporaryDirectory() as directory:
        listings = pathlib.Path(directory)
        (listings / "managers-tcp").write_text("".join(f"{LOCALHOST}:{start}\n" for start in manager_starts))
        (listings / "managers-udp").write_text("".join(f"{LOCALHOST}:{start + 1}\n" for start in manager_starts))
        locator_flags = (
            "--managers", f"file://{listings / 'managers-tcp'}",
            "--manager-udp", f"file://{listings / 'managers-udp'}",
        )
        managers = [
            node_at(
                "manager", start, run_marker, "--datacenter", DATACENTER, *locator_flags,
                "--cohort-size", str(COHORT_SIZE), "--log-level", "debug",
            )
            for start in manager_starts
        ]
        misconfigured = node_at(
            "manager", manager_starts[0], run_marker, "--datacenter", DATACENTER, *locator_flags,
            # The first manager's own address -- it resolves the cohort and
            # refuses before binding anything.
            "--cohort-size", str(COHORT_SIZE + 1),
        )
        nodes = [*managers, misconfigured]

        try:
            await boot(*managers)
            for manager in managers:
                assert await manager.wait_for_output("formed: member", within=BOOT_TIMEOUT_SECONDS), (
                    f"manager {manager.address} never formed its membership"
                )

            await misconfigured.start()
            # Resolution retries until the boot timeout, then refuses.
            await asyncio.wait_for(misconfigured.process.wait(), timeout=2 * BOOT_TIMEOUT_SECONDS)
            await asyncio.wait_for(misconfigured._pump, timeout=BOOT_TIMEOUT_SECONDS)
            output = "".join(misconfigured.lines)
            assert misconfigured.process.returncode != 0
            assert f"not the cohort's {COHORT_SIZE + 1}" in output, output

        finally:
            await kill_remaining(nodes)
