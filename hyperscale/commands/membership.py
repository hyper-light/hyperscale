import sys
from typing import Literal

from hyperscale.commands.cli import AssertSet, command
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.distributed.cluster import ClusterJoinError
from hyperscale.distributed.env import Env as HyperscaleEnv
from hyperscale.distributed.nodes import HyperscaleClient
from hyperscale.logging import LoggingConfig, LogLevelName

from .run.node_address import parse_node_address
from .run.shared import resolve_auth_secret

ClusterModeName = Literal["open", "frozen", "read-only"]


def _exit_with_error(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


@command(
    display_help_on_error=False,
    shortnames={"host": "H", "timeout": "T"},
)
async def membership(
    node: str = None,
    mode: AssertSet[ClusterModeName] = None,
    host: str = "127.0.0.1",
    port: int = 8500,
    timeout: str | None = None,
    acm_secret: str | None = None,
    log_level: AssertSet[LogLevelName] = "fatal",
):
    """
    Set a manager or gate cluster's mode: open; frozen, so no member joins
    or leaves (for maintenance); or read-only, frozen and refusing new jobs
    (for upgrades). Jobs already running go on.

    @param node The host:port (TCP) of any manager or gate of the cluster
    @param mode open, frozen or read-only
    @param host The local address this command listens on for the reply
    @param port The local TCP port this command listens on for the reply
    @param timeout How long to wait (defaults to the cluster's standard TCP timeout plus a formation interval)
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET)
    @param log_level The log level to use
    """
    try:
        node_addr = parse_node_address(node)

    except ValueError as address_error:
        _exit_with_error(str(address_error))

    if mode is None:
        _exit_with_error("--mode is required: open, frozen or read-only")

    LoggingConfig().update(log_level=log_level.data, log_output="stderr")
    env = HyperscaleEnv(
        MERCURY_SYNC_AUTH_SECRET=resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
    )
    # A member passes the change to its leader (one standard request), which
    # commits it (at most one formation interval, its proposal timeout).
    mode_timeout = (
        TimeParser(timeout).time
        if timeout
        else max(env.MANAGER_TCP_TIMEOUT_STANDARD, env.GATE_TCP_TIMEOUT_STANDARD)
        + env.CLUSTER_FORMATION_INTERVAL_SECONDS
    )

    client = HyperscaleClient(host=host, port=port, env=env)
    await client.start()

    try:
        reply = await client.set_cluster_mode(node_addr, mode.data, mode_timeout)

    except ClusterJoinError as mode_error:
        _exit_with_error(f"mode change failed via {node_addr[0]}:{node_addr[1]}: {mode_error}")

    finally:
        await client.stop()

    if not reply.applied:
        _exit_with_error(f"mode change refused via {node_addr[0]}:{node_addr[1]}: {reply.refusal}")

    print(f"cluster mode {reply.mode}")
