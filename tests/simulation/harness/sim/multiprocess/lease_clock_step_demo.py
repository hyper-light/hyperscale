"""
Leases across a wall-clock step (the NTP-step model,
``VirtualClock.set_wall_offset``) over the multi-process coordinator.

Two lease kinds, each the picklable child entry of its scenarios:

* AD-52 section 11 leader leases: three peered managers with
  ``RAFT_LEADER_LEASES_ENABLED``. Every member asks its cluster
  membership group for the cluster's status every ``read_interval``
  seconds -- ``ClusterMembership.handle_status``, the linearizable read a
  ``cluster_status`` request runs, with ``forwarded`` set so a member
  that does not lead answers locally instead of passing the read on --
  and records how the read was served: from the leader's lease, by a
  round of heartbeats, not at all, or refused for not leading.
* The gate's per-job ``JobLease``: one gate fronting one datacenter. The
  gate's acquisitions and renewals are recorded as they are granted, and
  every ``watch_interval`` seconds the lease of each job the gate holds
  is sampled: whether it is active and how long it has left.

Rows carry booleans, counts, terms and virtual times only -- never a
node, job or member id, which are fresh per run -- so every log stays
inside the replay contract.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
from pathlib import Path

from hyperscale.distributed.cluster.models import ClusterStatusReply, ClusterStatusRequest
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.manager.server import ManagerServer

from .chaos_cluster_demo import apply_clock_skew_schedule
from .peered_manager_demo import _env
from .workflow_lifecycle_demo import _build_steady_workflow

# The read a member issues locally: answered by this member, never passed
# on to the leader it names.
_LOCAL_STATUS_READ = ClusterStatusRequest(forwarded=True).dump()


def _classify_status_read(group, reply: ClusterStatusReply, lease_reads_before: int) -> str:
    """How one status read was served: ``lease`` (the leader answered from
    its lease, no round of its own), ``round`` (the leader confirmed its
    leadership with a quorum's answer), ``unserved`` (it led but could
    not confirm), or ``not-leader``."""
    if reply.served:
        return "lease" if group.metrics()["lease_reads"] > lease_reads_before else "round"
    return "not-leader" if reply.refusal == "not the membership group's leader" else "unserved"


def lease_reading_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    peer_tcp_addresses,
    peer_udp_addresses,
    clock_skew_schedule,
    read_interval,
) -> None:
    """Manager child with leader leases on, its wall clock following
    ``clock_skew_schedule`` (``("wall_skew", at, delta_seconds)`` steps),
    reading the cluster's status every ``read_interval`` seconds. Logs
    ``("leads", bool, term, t)`` when its membership-group leadership or
    term changes, ``("read", outcome, t)`` when a read's outcome differs
    from the last one's, and ``("lease-reads", first_t, last_t, term)`` for
    each unbroken run of reads served from the lease."""
    apply_clock_skew_schedule(context, clock_skew_schedule)
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(RAFT_LEADER_LEASES_ENABLED=True),
        dc_id=datacenter_id,
        seed_managers=list(peer_tcp_addresses),
        manager_udp_peers=list(peer_udp_addresses),
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    membership = manager._cluster_membership

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

    async def read_status() -> None:
        last_leadership: tuple[bool, int] | None = None
        last_outcome: str | None = None
        # Where in the log the unbroken run of lease-served reads in
        # progress is recorded (rewritten in place as it grows), or None.
        lease_run_row: int | None = None
        while True:
            await asyncio.sleep(read_interval)
            if (group := membership._group) is None:
                continue
            leadership = (group.is_leader(), group.current_term)
            if leadership != last_leadership:
                last_leadership = leadership
                log.append(("leads", leadership[0], leadership[1], round(context.loop.time(), 6)))

            # The read is judged at its start: a lease read returns at once,
            # a round-confirmed one after its quorum answered.
            read_at = round(context.loop.time(), 6)
            lease_reads_before = group.metrics()["lease_reads"]
            reply = ClusterStatusReply.load(await membership.handle_status(_LOCAL_STATUS_READ))
            outcome = _classify_status_read(group, reply, lease_reads_before)

            if outcome != "lease":
                lease_run_row = None
            elif lease_run_row is None:
                lease_run_row = len(log)
                log.append(("lease-reads", read_at, read_at, group.current_term))
            else:
                _tag, first_read_at, _last_read_at, term = log[lease_run_row]
                log[lease_run_row] = ("lease-reads", first_read_at, read_at, term)

            if outcome != last_outcome:
                last_outcome = outcome
                log.append(("read", outcome, read_at))

    context.loop.create_task(run())
    context.loop.create_task(read_status())


def job_lease_gate_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_managers,
    datacenter_manager_udp,
    clock_skew_schedule,
    job_lease_duration,
    lease_cleanup_interval,
    job_retention_seconds,
    watch_interval,
) -> None:
    """Gate child (no gate peers) whose wall clock follows
    ``clock_skew_schedule``, with ``JOB_LEASE_DURATION``,
    ``JOB_LEASE_CLEANUP_INTERVAL`` and ``FAILED_JOB_MAX_AGE`` (the
    retention of a job and of its ended lease) set from
    ``job_lease_duration``, ``lease_cleanup_interval`` and
    ``job_retention_seconds``. Every ``watch_interval`` seconds it samples the
    lease of each job it holds one for, logging ``("lease-sample", granted,
    active, remaining, t)``: ``granted`` is whether the lease was granted
    anew (acquired or renewed) since the last sample, ``active`` and
    ``remaining`` are the lease's own verdict -- ``JobLease.is_active`` and
    ``JobLease.remaining_seconds``. ``("lease-released", t)`` marks the
    sample that found it released, and ``("lease-records", leases,
    fence_tokens, t)`` how many lease records and fence tokens the gate
    holds, whenever either changes."""
    apply_clock_skew_schedule(context, clock_skew_schedule)
    gate = GateServer(
        host,
        tcp_port,
        udp_port,
        _env(
            JOB_LEASE_DURATION=job_lease_duration,
            JOB_LEASE_CLEANUP_INTERVAL=lease_cleanup_interval,
            FAILED_JOB_MAX_AGE=job_retention_seconds,
        ),
        dc_id="global",
        datacenter_managers=datacenter_managers,
        datacenter_manager_udp=datacenter_manager_udp,
        gate_peers=[],
        gate_udp_peers=[],
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    lease_manager = gate._job_lease_manager

    async def run() -> None:
        await gate.start()
        log.append(("gate-started", round(context.loop.time(), 6)))

    async def watch_leases() -> None:
        # A grant -- acquisition or renewal -- moves the lease's expiry:
        # only as the marker that one happened, never as a bound.
        last_expiries: dict[str, float] = {}
        last_records: tuple[int, int] | None = None
        while True:
            await asyncio.sleep(watch_interval)
            sampled_at = round(context.loop.time(), 6)
            if (records := (len(lease_manager._leases), len(lease_manager._fence_tokens))) != last_records:
                last_records = records
                log.append(("lease-records", *records, sampled_at))
            for job_id, lease in list(lease_manager._leases.items()):
                if job_id in last_expiries and lease.state.value == "released":
                    del last_expiries[job_id]
                    log.append(("lease-released", sampled_at))
                    continue
                if lease.state.value == "released":
                    continue
                granted = last_expiries.get(job_id) != lease.expires_at
                last_expiries[job_id] = lease.expires_at
                log.append(
                    (
                        "lease-sample",
                        granted,
                        lease.is_active(),
                        round(lease.remaining_seconds(), 6),
                        sampled_at,
                    )
                )

    context.loop.create_task(run())
    context.loop.create_task(watch_leases())


def steady_gate_client_entry(
    context,
    host,
    port,
    gate_tcp_address,
    workflow_duration_seconds,
    job_timeout_seconds,
    wait_timeout_seconds,
) -> None:
    """Client child: submit one ``SimSteadyWorkflow`` of
    ``workflow_duration_seconds`` through the gate and await its terminal.
    Records ``("submit-rejected", error type, t)`` per refused attempt,
    ``("job-submitted", t)`` and ``("job-finished", status, t)``."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        gates=[gate_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    steady_workflow_class = _build_steady_workflow(workflow_duration_seconds)

    async def run() -> None:
        await client.start()
        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], steady_workflow_class())],
                    vus=2,
                    timeout_seconds=job_timeout_seconds,
                )
            except Exception as submit_error:
                log.append(("submit-rejected", type(submit_error).__name__, round(context.loop.time(), 6)))
                await asyncio.sleep(1.0)
        log.append(("job-submitted", round(context.loop.time(), 6)))
        result = await client.wait_for_job(job_id, timeout=wait_timeout_seconds)
        log.append(("job-finished", result.status, round(context.loop.time(), 6)))

    context.loop.create_task(run())
