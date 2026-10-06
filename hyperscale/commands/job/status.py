import sys
from typing import Literal

from hyperscale.commands.cli import AssertSet, command
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.distributed.cluster import ClusterJoinError
from hyperscale.distributed.env import Env as HyperscaleEnv
from hyperscale.distributed.models import ReadConsistency
from hyperscale.distributed.nodes import HyperscaleClient
from hyperscale.logging import LoggingConfig, LogLevelName

from ..run.node_address import parse_node_address
from ..run.shared import resolve_auth_secret


ReadConsistencyName = Literal["eventual", "session", "bounded_staleness", "strong"]


def _exit_with_error(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


@command(
    display_help_on_error=False,
    shortnames={"host": "H", "timeout": "T", "job_id": "j"},
)
async def status(
    job_id: str = None,
    gates: list[str] = [],
    managers: list[str] = [],
    host: str = "127.0.0.1",
    port: int = 8500,
    timeout: str | None = None,
    consistency: AssertSet[ReadConsistencyName] = "eventual",
    max_staleness: str | None = None,
    acm_secret: str | None = None,
    log_level: AssertSet[LogLevelName] = "fatal",
):
    """
    Show a job's status: asks each gate, then each manager, until one knows
    the job (a manager answers from its durable ledger after the job ended).

    @param job_id The id of the job
    @param gates The TCP host:port of gates to ask
    @param managers The TCP host:port of managers to ask (a gateless cluster)
    @param host The local address this command listens on for the reply
    @param port The local TCP port this command listens on for the reply
    @param timeout How long to wait on each node (defaults to its tier's standard TCP timeout)
    @param consistency How current the status must be: eventual (what the node asked holds), session, bounded_staleness (the job leader's view, no older than --max-staleness) or strong (the job leader's view, confirmed with a quorum)
    @param max_staleness The oldest view a bounded_staleness read accepts (e.g. 5s)
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET, else the per-user cluster cookie)
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
        MERCURY_SYNC_AUTH_SECRET=await resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
    )
    targets = [(address, env.GATE_TCP_TIMEOUT_STANDARD) for address in gate_addrs] + [
        (address, env.MANAGER_TCP_TIMEOUT_STANDARD) for address in manager_addrs
    ]

    client = HyperscaleClient(host=host, port=port, env=env)
    await client.start()
    unreachable: list[str] = []
    try:
        for address, tier_timeout in targets:
            try:
                job_status = await client.query_job_status(
                    address,
                    job_id,
                    TimeParser(timeout).time if timeout else tier_timeout,
                    consistency=ReadConsistency(consistency.data),
                    max_staleness_seconds=TimeParser(max_staleness).time if max_staleness else 0.0,
                )
            except ClusterJoinError as status_error:
                unreachable.append(str(status_error))
                continue
            if job_status is not None:
                break
        else:
            job_status = None
    finally:
        await client.stop()

    if job_status is None:
        _exit_with_error(
            f"no node asked knows job {job_id}"
            + (f" ({len(unreachable)} unreachable: {'; '.join(unreachable)})" if unreachable else "")
        )

    print(f"job {job_status.job_id}: {job_status.status}")
    print(f"  completed {job_status.total_completed}  failed {job_status.total_failed}  rate {job_status.overall_rate:.2f}/s")
    print(f"  elapsed {job_status.elapsed_seconds:.1f}s  progress {job_status.progress_percentage:.1f}%")
    for datacenter in job_status.datacenters:
        print(
            f"  {datacenter.datacenter}: {datacenter.status}  completed {datacenter.total_completed}"
            f"  failed {datacenter.total_failed}"
        )
    for error in job_status.errors:
        print(f"  error: {error}")
