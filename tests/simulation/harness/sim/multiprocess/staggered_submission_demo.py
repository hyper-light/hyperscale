"""
Staggered starts across a gate cluster (SCENARIOS §7 "Staggered.
Submissions at offsets across multiple gates concurrently") -- the
picklable child entries of the staggered-workload scenario.

``gated_lifecycle_manager_entry`` is a manager attached to the whole gate
tier with its AD-54 workflow lifecycle observed
(``observe_workflow_lifecycle``), completed jobs retained and swept on
the scenario's schedule. ``gate_paced_client_entry`` submits through ONE
gate on ``sustained_submission_demo.run_paced_jobs``' schedule; the
scenario admits one per gate, each offset from the last -- the offsets
anchored on the first client's first acceptance, an event of the run.

Rows carry ordinals, statuses, counts and virtual times only -- inside
the replay contract.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

from pathlib import Path

from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.manager.server import ManagerServer

from .child_context import ChildContext
from .sustained_submission_demo import run_paced_jobs
from .workflow_lifecycle_demo import _env, observe_workflow_lifecycle


def gated_lifecycle_manager_entry(
    context: ChildContext,
    host: str,
    tcp_port: int,
    udp_port: int,
    datacenter_id: str,
    gate_tcp_addresses: list[tuple[str, int]],
    gate_udp_addresses: list[tuple[str, int]],
    job_retention_seconds: float,
    job_cleanup_interval_seconds: float,
) -> None:
    """Manager child registered with every gate of the tier, its workflow
    lifecycle observed, completed jobs retained ``job_retention_seconds``
    and swept every ``job_cleanup_interval_seconds``."""
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(
            COMPLETED_JOB_MAX_AGE=job_retention_seconds,
            FAILED_JOB_MAX_AGE=job_retention_seconds,
            JOB_CLEANUP_INTERVAL=job_cleanup_interval_seconds,
        ),
        dc_id=datacenter_id,
        gate_addrs=gate_tcp_addresses,
        gate_udp_addrs=gate_udp_addresses,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list[tuple[object, ...]] = []
    context.set_result(log)
    context.loop.create_task(manager.start())
    observe_workflow_lifecycle(context, manager, log)


def gate_paced_client_entry(
    context: ChildContext,
    host: str,
    port: int,
    gate_tcp_address: tuple[str, int],
    jobs_per_second: float,
    total_jobs: int,
    job_timeout_seconds: float,
) -> None:
    """Client child submitting ``total_jobs`` ``SimPingWorkflow`` jobs
    through the gate at ``gate_tcp_address``, one every ``1 /
    jobs_per_second`` virtual seconds from its first acceptance
    (``run_paced_jobs``)."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        gates=[gate_tcp_address],
        **context.sim_kwargs(),
    )
    log: list[tuple[object, ...]] = []
    context.set_result(log)
    run_paced_jobs(context, client, log, jobs_per_second, total_jobs, job_timeout_seconds)
