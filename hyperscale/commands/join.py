import sys

from hyperscale.commands.cli import AssertSet, command
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.distributed.cluster import ClusterJoinError
from hyperscale.distributed.env import Env as HyperscaleEnv
from hyperscale.distributed.nodes import HyperscaleClient
from hyperscale.distributed.nodes.worker.registration import (
    REGISTRATION_ATTEMPT_TIMEOUT_SECONDS,
)
from hyperscale.logging import LoggingConfig, LogLevelName

from .run.node_address import parse_node_address
from .run.shared import resolve_auth_secret


def default_join_timeout_seconds(env: HyperscaleEnv) -> float:
    """How long a join may take on its slowest path, from configuration.

    A worker joining a manager runs the full registration retry budget:
    ``WORKER_REGISTRATION_MAX_RETRIES + 1`` attempts bounded by
    ``REGISTRATION_ATTEMPT_TIMEOUT_SECONDS`` each, plus full-jitter
    exponential backoff that sums to at most
    ``base * (2**retries - 1)``. A manager joining a gate is a single
    ``MANAGER_TCP_TIMEOUT_STANDARD`` hop. The operator waits for the
    slower of the two.
    """
    registration_retries = env.WORKER_REGISTRATION_MAX_RETRIES
    worker_join_budget = (
        (registration_retries + 1) * REGISTRATION_ATTEMPT_TIMEOUT_SECONDS
        + env.WORKER_REGISTRATION_BASE_DELAY * (2**registration_retries - 1)
    )
    return max(worker_join_budget, env.MANAGER_TCP_TIMEOUT_STANDARD)


def _exit_with_error(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


@command(
    display_help_on_error=False,
    shortnames={"host": "H", "timeout": "T"},
)
async def join(
    node: str = None,
    target: str = None,
    host: str = "127.0.0.1",
    port: int = 8500,
    timeout: str | None = None,
    acm_secret: str | None = None,
    log_level: AssertSet[LogLevelName] = "fatal",
):
    """
    Tell a running node to join another: a worker joins a manager, a
    manager joins a gate, or a gate joins a manager.

    @param node The host:port (TCP) of the node that should join
    @param target The host:port (TCP) of the node to join
    @param host The local address this command listens on for the reply
    @param port The local TCP port this command listens on for the reply
    @param timeout How long to wait for the join (defaults to the slowest join path's configured budget)
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET)
    @param log_level The log level to use
    """
    try:
        node_addr = parse_node_address(node)
        target_addr = parse_node_address(target)

    except ValueError as address_error:
        _exit_with_error(str(address_error))

    LoggingConfig().update(log_level=log_level.data, log_output="stderr")
    env = HyperscaleEnv(
        MERCURY_SYNC_AUTH_SECRET=resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
    )
    join_timeout = (
        TimeParser(timeout).time if timeout else default_join_timeout_seconds(env)
    )

    client = HyperscaleClient(host=host, port=port, env=env)
    await client.start()

    joined = f"{node_addr[0]}:{node_addr[1]} -> {target_addr[0]}:{target_addr[1]}"
    try:
        join_response = await client.join_node(node_addr, target_addr, join_timeout)

    except ClusterJoinError as join_error:
        _exit_with_error(f"join failed ({joined}): {join_error}")

    finally:
        await client.stop()

    if not join_response.accepted:
        _exit_with_error(f"join failed ({joined}): {join_response.error}")

    print(f"joined {join_response.node_role} {joined}")
