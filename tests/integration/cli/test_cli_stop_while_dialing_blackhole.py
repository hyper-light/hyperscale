"""
E2E (live CLI): a manager stops within its shutdown timeout while it is
dialing an address that drops packets -- a real `hyperscale run manager`
process.

In Kubernetes a restarted pod's old IP drops packets: a connect to it is
neither accepted nor refused, it waits out the kernel's SYN retries
(minutes). The node's TCP client connected on an executor thread
(`run_in_executor(None, socket.connect, ...)`); cancelling the caller left
the thread blocked, and `asyncio.run`, shutting down the default executor
as the command returned, waited for it. Managers stopped seconds after a
peer restarted outlived their termination grace and were SIGKILLed.

A manager's cohort here names a second member at 192.0.2.1 (TEST-NET-1,
documentation-only, routed nowhere): its formation rounds keep dialing
it. Sent SIGTERM, it must exit within the --shutdown-timeout it was given.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import signal
import socket
import time

import pytest

from hyperscale.distributed.env import Env
from tests.integration.cli.node_processes import (
    BOOT_MARKERS,
    BOOT_TIMEOUT_SECONDS,
    LOCALHOST,
    NODE_BLOCK,
    kill_remaining,
    node_at,
    reserve_port_blocks,
)

DATACENTER = "dc-blackhole"
# TEST-NET-1 (RFC 5737): reserved for documentation, never routed.
BLACKHOLE_HOST = "192.0.2.1"
SHUTDOWN_TIMEOUT_SECONDS = 10
# Long enough to see a connect to the blackhole is neither accepted nor
# refused, far below the shutdown timeout.
BLACKHOLE_PROBE_SECONDS = 2.0


@pytest.fixture
def run_marker() -> str:
    return f"cli-blackhole-{time.monotonic_ns()}"


def address_drops_packets(host: str, port: int) -> bool:
    """Whether a connect to ``host:port`` hangs (neither accepted nor
    refused) for the probe window on this machine's network."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.settimeout(BLACKHOLE_PROBE_SECONDS)
    try:
        probe.connect((host, port))
    except TimeoutError:
        return True
    except OSError:
        return False
    finally:
        probe.close()
    return False


async def test_a_manager_dialing_a_blackhole_stops_within_its_shutdown_timeout(run_marker: str) -> None:
    (manager_start,) = reserve_port_blocks([NODE_BLOCK])
    blackhole_tcp_port = manager_start + 100
    if not address_drops_packets(BLACKHOLE_HOST, blackhole_tcp_port):
        pytest.skip(f"{BLACKHOLE_HOST} does not drop packets on this network: nothing here dials a blackhole")

    manager = node_at(
        "manager",
        manager_start,
        run_marker,
        "--datacenter", DATACENTER,
        "--managers", f"{LOCALHOST}:{manager_start}", f"{BLACKHOLE_HOST}:{blackhole_tcp_port}",
        "--manager-udp", f"{LOCALHOST}:{manager_start + 1}", f"{BLACKHOLE_HOST}:{blackhole_tcp_port + 1}",
        "--shutdown-timeout", f"{SHUTDOWN_TIMEOUT_SECONDS}s",
        "--log-level", "debug",
    )
    try:
        await manager.start()
        assert await manager.wait_for_output(BOOT_MARKERS["manager"], within=BOOT_TIMEOUT_SECONDS), (
            "".join(manager.lines[-30:])
        )
        # A formation round runs every formation interval and greets the
        # cohort's other founder: one interval after boot, a connect to the
        # blackhole is under way (it can only end by timing out).
        await asyncio.sleep(Env().CLUSTER_FORMATION_INTERVAL_SECONDS)

        stopping_since = time.monotonic()
        exited, survivors = await manager.stop(signal.SIGTERM, whole_group=False)
        stopped_after = time.monotonic() - stopping_since

        assert exited and survivors == [], f"the manager did not stop: {survivors}"
        assert stopped_after < SHUTDOWN_TIMEOUT_SECONDS, (
            f"the manager took {stopped_after:.1f}s to stop, past its {SHUTDOWN_TIMEOUT_SECONDS}s shutdown timeout"
        )
    finally:
        await kill_remaining([manager])
