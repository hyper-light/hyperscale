import sys

from hyperscale.commands.cli import AssertSet, command
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.distributed.cluster import ClusterJoinError
from hyperscale.distributed.env import Env as HyperscaleEnv
from hyperscale.distributed.nodes import HyperscaleClient
from hyperscale.logging import LoggingConfig, LogLevelName

from .run.node_address import parse_node_address
from .run.shared import resolve_auth_secret


def _exit_with_error(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


@command(
    display_help_on_error=False,
    shortnames={"host": "H", "timeout": "T"},
)
async def remove(
    node: str = None,
    member: str = None,
    host: str = "127.0.0.1",
    port: int = 8500,
    timeout: str | None = None,
    acm_secret: str | None = None,
    log_level: AssertSet[LogLevelName] = "fatal",
):
    """
    Remove a manager or gate that is gone for good from its cluster's
    membership now, instead of waiting out the tombstone retention. Refused
    while the member still answers: a live node drains itself when stopped.

    @param node The host:port (TCP) of any manager or gate of the cluster
    @param member The host:port (TCP) of the member to remove
    @param host The local address this command listens on for the reply
    @param port The local TCP port this command listens on for the reply
    @param timeout How long to wait (defaults to the cluster's standard TCP timeout)
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET)
    @param log_level The log level to use
    """
    try:
        node_addr = parse_node_address(node)
        member_addr = parse_node_address(member)

    except ValueError as address_error:
        _exit_with_error(str(address_error))

    LoggingConfig().update(log_level=log_level.data, log_output="stderr")
    env = HyperscaleEnv(
        MERCURY_SYNC_AUTH_SECRET=resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
    )
    # The leader greets the member's address (at most the tier's standard
    # request timeout) and commits the release (at most one formation
    # interval, its proposal timeout); per node asked.
    remove_timeout = (
        TimeParser(timeout).time
        if timeout
        else max(env.MANAGER_TCP_TIMEOUT_STANDARD, env.GATE_TCP_TIMEOUT_STANDARD)
        + env.CLUSTER_FORMATION_INTERVAL_SECONDS
    )

    client = HyperscaleClient(host=host, port=port, env=env)
    await client.start()

    target = f"{member_addr[0]}:{member_addr[1]} via {node_addr[0]}:{node_addr[1]}"
    try:
        reply = await client.remove_cluster_member(node_addr, member_addr, remove_timeout)

    except ClusterJoinError as remove_error:
        _exit_with_error(f"remove failed ({target}): {remove_error}")

    finally:
        await client.stop()

    if not reply.released:
        _exit_with_error(f"remove refused ({target}): {reply.refusal}")

    print(f"removed {reply.released_member_id}")
