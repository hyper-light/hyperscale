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
async def resize(
    node: str = None,
    add: str | None = None,
    remove: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8500,
    timeout: str | None = None,
    acm_secret: str | None = None,
    log_level: AssertSet[LogLevelName] = "fatal",
):
    """
    Grow or shrink a manager or gate cluster's cohort by one address. Then
    launch any new node with the new cohort, and relaunch every other
    member with it (each drains as it stops) before the next resize: the
    cluster refuses one while a member still runs with an older cohort.

    @param node The host:port (TCP) of any manager or gate of the cluster
    @param add The host:port (TCP) to add to the cohort
    @param remove The host:port (TCP) to remove from the cohort
    @param host The local address this command listens on for the reply
    @param port The local TCP port this command listens on for the reply
    @param timeout How long to wait (defaults to the cluster's standard TCP timeout, twice, plus a formation interval)
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET, else the per-user cluster cookie)
    @param log_level The log level to use
    """
    if (add is None) == (remove is None):
        _exit_with_error("give exactly one of --add or --remove")

    try:
        node_addr = parse_node_address(node)
        member_addr = parse_node_address(add if add is not None else remove)

    except ValueError as address_error:
        _exit_with_error(str(address_error))

    LoggingConfig().update(log_level=log_level.data, log_output="stderr")
    env = HyperscaleEnv(
        MERCURY_SYNC_AUTH_SECRET=await resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
    )
    # The leader greets every voter (one standard request, in parallel),
    # then commits the resize (at most one formation interval); a member
    # that is not the leader adds a hop of one standard request.
    standard_timeout = max(env.MANAGER_TCP_TIMEOUT_STANDARD, env.GATE_TCP_TIMEOUT_STANDARD)
    resize_timeout = (
        TimeParser(timeout).time
        if timeout
        else 2 * standard_timeout + env.CLUSTER_FORMATION_INTERVAL_SECONDS
    )

    client = HyperscaleClient(host=host, port=port, env=env)
    await client.start()

    change = f"{'add' if add is not None else 'remove'} {member_addr[0]}:{member_addr[1]}"
    try:
        reply = await client.resize_cluster(node_addr, member_addr, add is not None, resize_timeout)

    except ClusterJoinError as resize_error:
        _exit_with_error(f"resize failed ({change}): {resize_error}")

    finally:
        await client.stop()

    if not reply.applied:
        _exit_with_error(f"resize refused ({change}): {reply.refusal}")

    print(f"cohort {' '.join(reply.cohort)}")
