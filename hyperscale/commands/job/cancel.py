import sys

from hyperscale.commands.cli import AssertSet, command
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.distributed.env import Env as HyperscaleEnv
from hyperscale.distributed.nodes import HyperscaleClient
from hyperscale.logging import LoggingConfig, LogLevelName

from ..run.node_address import parse_node_address
from ..run.shared import resolve_auth_secret


def _exit_with_error(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


@command(
    display_help_on_error=False,
    shortnames={"host": "H", "timeout": "T", "job_id": "j"},
)
async def cancel(
    job_id: str = None,
    reason: str = "cancelled by operator",
    gates: list[str] = [],
    managers: list[str] = [],
    host: str = "127.0.0.1",
    port: int = 8500,
    timeout: str | None = None,
    acm_secret: str | None = None,
    log_level: AssertSet[LogLevelName] = "fatal",
):
    """
    Cancel a running job: the gates (or, gateless, the managers) given are
    asked in turn, following any redirect to the job's leader.

    @param job_id The id of the job
    @param reason Why the job is cancelled (recorded with the job)
    @param gates The TCP host:port of gates to ask
    @param managers The TCP host:port of managers to ask (a gateless cluster)
    @param host The local address this command listens on for the reply
    @param port The local TCP port this command listens on for the reply
    @param timeout How long to wait on each attempt (defaults to the slower tier's standard TCP timeout)
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET)
    @param log_level The log level to use
    """
    if not job_id:
        _exit_with_error("--job-id is required")
    try:
        gate_addrs = [parse_node_address(gate) for gate in gates]
        manager_addrs = [parse_node_address(manager) for manager in managers]
    except ValueError as address_error:
        _exit_with_error(str(address_error))
    if not gate_addrs and not manager_addrs:
        _exit_with_error("give --gates or --managers to ask")

    LoggingConfig().update(log_level=log_level.data, log_output="stderr")
    env = HyperscaleEnv(
        MERCURY_SYNC_AUTH_SECRET=resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
    )
    cancel_timeout = (
        TimeParser(timeout).time
        if timeout
        else max(env.GATE_TCP_TIMEOUT_STANDARD, env.MANAGER_TCP_TIMEOUT_STANDARD)
    )

    client = HyperscaleClient(
        host=host, port=port, env=env, gates=gate_addrs or None, managers=manager_addrs or None
    )
    await client.start()
    try:
        response = await client.cancel_job(job_id, reason=reason, timeout=cancel_timeout)
    except Exception as cancel_error:
        _exit_with_error(f"cancel of job {job_id} failed: {type(cancel_error).__name__}: {cancel_error}")
    finally:
        await client.stop()

    if response.already_completed:
        _exit_with_error(f"job {job_id} had already completed")
    if not response.success and not response.already_cancelled:
        _exit_with_error(f"cancel of job {job_id} refused: {response.error}")
    print(
        f"job {job_id} {'was already cancelled' if response.already_cancelled else 'cancelled'}"
        f" ({response.cancelled_workflow_count} workflows)"
    )
