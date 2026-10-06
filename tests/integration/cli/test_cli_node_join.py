"""
E2E: `hyperscale run worker|manager|gate` + `hyperscale join`, real processes.

Every node is a real `hyperscale run <role>` OS process (own session, real
sockets, real executor pool); every join is a real `hyperscale join`
process. Asserts:

1. All three roles boot standalone (no seeds / peers).
2. Cross-tier joins take effect on the TARGET, observed in its own output:
   worker->manager registration, manager->gate registration, and a
   gate->manager join after which the manager's heartbeats reach the gate.
3. Invalid joins are refused fast and readably: same-role peers, wrong
   role, self, unreachable node/target, malformed address.
4. Ctrl-C (SIGINT to the whole process group, like a terminal) and
   SIGTERM (to the parent only, like a supervisor) stop every node with
   zero surviving descendants and no orphaned executor processes.

Ceilings are derived from the nodes' own configuration, not chosen:
boot is bounded by the --boot-timeout each node is launched with; a
join reply by the CLI's configured join budget; the post-join heartbeat
by the manager's gate heartbeat interval plus that send's timeout.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import os
import signal
import time

import pytest

from hyperscale.commands.join import default_join_timeout_seconds
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.manager.config import create_manager_config_from_env
from tests.integration.cli.node_processes import (
    HYPERSCALE,
    LOCALHOST,
    NODE_BLOCK,
    boot,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    run_join,
    stop_all,
    worker_block,
)

ENV = Env()
JOIN_REPLY_BOUND_SECONDS = default_join_timeout_seconds(ENV)
_MANAGER_CONFIG = create_manager_config_from_env(LOCALHOST, 1, 2, ENV)
# The gate heartbeat loop sleeps one interval then sends with the short
# TCP timeout (manager/server.py _gate_heartbeat_loop), so the first
# heartbeat after a join lands within one interval plus that send.
HEARTBEAT_BOUND_SECONDS = (
    _MANAGER_CONFIG.heartbeat_interval_seconds
    + _MANAGER_CONFIG.tcp_timeout_short_seconds
)




CLIENT_BLOCK = 2  # `hyperscale join` client tcp + its udp (port + 1)
UNUSED_PORT_BLOCK = 1  # reserved and never bound: a guaranteed-closed port


@pytest.fixture
def run_marker() -> str:
    return f"cli-join-{os.getpid()}-{time.monotonic_ns()}"


async def test_cross_tier_joins_take_effect_on_targets(run_marker: str) -> None:
    worker_start, manager_a_start, manager_b_start, gate_start, *client_starts = (
        reserve_port_blocks([worker_block(2), NODE_BLOCK, NODE_BLOCK, NODE_BLOCK] + [CLIENT_BLOCK] * 3)
    )
    worker = node_at("worker", worker_start, run_marker, "--workers", "2", "--log-level", "info")
    manager_a = node_at("manager", manager_a_start, run_marker, "--log-level", "info")
    manager_b = node_at("manager", manager_b_start, run_marker, "--datacenter", "dc-b", "--log-level", "debug")
    gate = node_at("gate", gate_start, run_marker, "--log-level", "info")
    nodes = [worker, manager_a, manager_b, gate]

    try:
        await boot(*nodes)

        returncode, output = await run_join(worker.address, manager_a.address, client_starts[0])
        assert returncode == 0, output
        assert await manager_a.wait_for_output("registered with", within=JOIN_REPLY_BOUND_SECONDS)

        returncode, output = await run_join(manager_a.address, gate.address, client_starts[1])
        assert returncode == 0, output
        assert await gate.wait_for_output("Manager registered", within=JOIN_REPLY_BOUND_SECONDS)

        returncode, output = await run_join(gate.address, manager_b.address, client_starts[2])
        assert returncode == 0, output
        assert await manager_b.wait_for_output("Gate", "registered", within=JOIN_REPLY_BOUND_SECONDS)
        assert await manager_b.wait_for_output(
            "Sent heartbeat to 1/1 gates", within=HEARTBEAT_BOUND_SECONDS
        ), "manager accepted the gate but never heartbeated to it"

        await stop_all(nodes, signal.SIGINT, whole_group=True)

    finally:
        await kill_remaining(nodes)


@pytest.mark.parametrize(
    "case",
    [
        "manager_to_manager",
        "worker_to_gate",
        "worker_to_self",
        "unreachable_target",
        "unreachable_node",
        "malformed_address",
    ],
)
async def test_invalid_joins_are_refused_fast(case: str, run_marker: str) -> None:
    worker_start, manager_a_start, manager_b_start, gate_start, client_start, closed_port = (
        reserve_port_blocks(
            [worker_block(1), NODE_BLOCK, NODE_BLOCK, NODE_BLOCK, CLIENT_BLOCK, UNUSED_PORT_BLOCK]
        )
    )
    worker = node_at("worker", worker_start, run_marker, "--workers", "1")
    manager_a = node_at("manager", manager_a_start, run_marker)
    manager_b = node_at("manager", manager_b_start, run_marker)
    gate = node_at("gate", gate_start, run_marker)
    nodes = [worker, manager_a, manager_b, gate]

    joins = {
        "manager_to_manager": (manager_a.address, manager_b.address, "managers can only join gates"),
        "worker_to_gate": (worker.address, gate.address, "did not accept worker registration"),
        "worker_to_self": (worker.address, worker.address, "cannot join itself"),
        "unreachable_target": (manager_a.address, f"{LOCALHOST}:{closed_port}", "did not answer"),
        "unreachable_node": (f"{LOCALHOST}:{closed_port}", manager_a.address, "unreachable"),
        "malformed_address": (LOCALHOST, manager_a.address, "needs a port"),
    }
    node_address, target_address, expected_reason = joins[case]

    try:
        await boot(*nodes)

        started = time.monotonic()
        returncode, output = await run_join(node_address, target_address, client_start)
        elapsed = time.monotonic() - started

        assert returncode == 1, output
        assert expected_reason in output, output
        assert "Traceback" not in output, output
        # A refusal must come from an answer, not from waiting out the
        # join budget (wrong-role targets used to hang until timeout).
        assert elapsed < JOIN_REPLY_BOUND_SECONDS, f"refusal took {elapsed:.1f}s"

        await stop_all(nodes, signal.SIGINT, whole_group=True)

    finally:
        await kill_remaining(nodes)


@pytest.mark.parametrize(
    ("stop_signal", "whole_group"),
    [(signal.SIGINT, True), (signal.SIGTERM, False)],
    ids=["terminal-ctrl-c", "supervisor-sigterm"],
)
async def test_nodes_stop_without_leaks(
    stop_signal: signal.Signals,
    whole_group: bool,
    run_marker: str,
) -> None:
    worker_start, manager_start, gate_start, client_start = reserve_port_blocks(
        [worker_block(2), NODE_BLOCK, NODE_BLOCK, CLIENT_BLOCK]
    )
    worker = node_at("worker", worker_start, run_marker, "--workers", "2")
    manager = node_at("manager", manager_start, run_marker)
    gate = node_at("gate", gate_start, run_marker)
    nodes = [worker, manager, gate]

    try:
        await boot(*nodes)
        returncode, output = await run_join(worker.address, manager.address, client_start)
        assert returncode == 0, output

        await stop_all(nodes, stop_signal, whole_group)

    finally:
        await kill_remaining(nodes)
