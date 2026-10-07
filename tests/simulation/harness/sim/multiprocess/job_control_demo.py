"""
Job control (D-65 concurrency caps, D-67 noisy-job breaker): one gateless
manager, its workers and clients that each run jobs of one workflow class
-- the picklable child entries of the job-control scenarios.

The manager logs every admission decision its leader makes:
``("admission-admitted", job_class, t)`` and ``("admission-refused",
job_class, control, retry_after_seconds, t)`` (``control`` is
``concurrency_cap`` or ``noisy_job_breaker``); and, on change, the jobs its
admission control counts (``("counted-jobs", n, t)``) and the job classes
whose breaker it holds (``("quarantine", ((class, state), ...), t)``).

A worker refuses the first ``refused_dispatches`` dispatches of
``SimNoisyWorkflow`` -- a test whose workers cannot start it, until they
can -- and logs ``("noisy-dispatch-refused", count, t)`` for each and
``("dispatch-run", is_noisy, t)`` for each dispatch it runs.

A client submits ``job_count`` jobs of one workflow class, one after the
other, and logs ``("job-accepted", ordinal, t)`` and ``("job-finished",
ordinal, status, t)``; a submission call that gives up is logged
(``("submit-rejected", ordinal, error type, t)``) and made again.

Rows carry class names, ordinals, counts, statuses and virtual times only
-- inside the replay contract.
"""

import asyncio
import sys
from pathlib import Path

import cloudpickle

from hyperscale.distributed.jobs.job_admission_refused_error import JobAdmissionRefusedError
from hyperscale.distributed.jobs.job_class_circuit_breaker import NOISY_JOB_BREAKER_CONTROL
from hyperscale.distributed.jobs.job_concurrency_caps import CONCURRENCY_CAP_CONTROL
from hyperscale.distributed.jobs.job_shape import job_class_name
from hyperscale.distributed.jobs.workflow_dependencies import workflow_name
from hyperscale.distributed.models import JobAck
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.gate import GateServer
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.nodes.worker.server import WorkerServer
from hyperscale.graph import Workflow, step

from .job_dispatch_demo import SimPingWorkflow
from .peered_manager_demo import _env

WATCH_INTERVAL_SECONDS = 0.5


class SimNoisyWorkflow(Workflow):
    """``SimPingWorkflow``'s shape under another name: the class the
    workers refuse to start while the scenario's fault lasts."""

    vus = 2
    duration = "2s"

    @step()
    async def ping_action(self) -> dict[str, str]:
        await asyncio.sleep(0.5)
        return {"status": "ok"}


class SimLongWorkflow(Workflow):
    """Two VUs for forty seconds -- the step outlasts the duration, so the
    runner cuts it there: a job that holds its two cores that long."""

    vus = 2
    duration = "40s"

    @step()
    async def hold_action(self) -> dict[str, str]:
        await asyncio.sleep(60.0)
        return {"status": "ok"}


# The workflow classes travel by value, as a client's own workflows do:
# the manager's restricted unpickler admits no test module by reference.
cloudpickle.register_pickle_by_value(sys.modules[__name__])

WORKFLOW_CLASSES: dict[str, type[Workflow]] = {
    "ping": SimPingWorkflow,
    "noisy": SimNoisyWorkflow,
    "long": SimLongWorkflow,
}


def job_control_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    env_overrides,
    gate_tcp_addresses=None,
    gate_udp_addresses=None,
    peer_tcp_addresses=(),
    peer_udp_addresses=(),
) -> None:
    """Manager child with ``env_overrides`` applied (the caps under test),
    gateless unless ``gate_*_addresses`` name its gates, peered with
    ``peer_*_addresses``; logs its admission decisions, its admission
    control's counted jobs and quarantined classes, and whether it leads
    its datacenter (``("leader", is_leader, t)``)."""
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(**env_overrides),
        dc_id=datacenter_id,
        gate_addrs=gate_tcp_addresses,
        gate_udp_addrs=gate_udp_addresses,
        seed_managers=list(peer_tcp_addresses) or None,
        manager_udp_peers=list(peer_udp_addresses) or None,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    clear_refused_record_and_admit = manager._clear_refused_record_and_admit

    async def observed_admission(submission, workflows) -> None:
        job_class = job_class_name(workflow_name(instance) for _workflow_id, _dependencies, instance in workflows)
        try:
            await clear_refused_record_and_admit(submission, workflows)
        except JobAdmissionRefusedError as refusal:
            ack = JobAck.load(refusal.ack)
            control = NOISY_JOB_BREAKER_CONTROL if "quarantined" in ack.error else CONCURRENCY_CAP_CONTROL
            log.append(
                (
                    "admission-refused",
                    job_class,
                    control,
                    round(ack.retry_after_seconds, 6),
                    round(context.loop.time(), 6),
                )
            )
            raise
        log.append(("admission-admitted", job_class, round(context.loop.time(), 6)))

    manager._clear_refused_record_and_admit = observed_admission

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

    async def watch() -> None:
        last_values: dict[str, object] = {}
        while True:
            admission_control = manager._job_admission_control
            values = {
                "counted-jobs": len(admission_control.counted_job_ids()),
                "quarantine": tuple(sorted(admission_control.quarantined_job_classes().items())),
                "leader": manager.is_leader(),
            }
            for tag, value in values.items():
                if last_values.get(tag) != value:
                    last_values[tag] = value
                    log.append((tag, value, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())
    context.loop.create_task(watch())


def job_control_worker_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    seed_manager_addresses,
    total_cores,
    refused_dispatches,
) -> None:
    """Worker child with ``WORKER_MAX_CORES`` = ``total_cores`` that
    refuses the first ``refused_dispatches`` dispatches of
    ``SimNoisyWorkflow`` (its start raises: the dispatch is answered
    not-taken, as a worker that cannot load a workflow answers)."""
    worker = WorkerServer(
        host,
        tcp_port,
        udp_port,
        _env(WORKER_MAX_CORES=total_cores),
        dc_id=datacenter_id,
        seed_managers=list(seed_manager_addresses),
        **context.sim_kwargs(),
        process_spawner=context,
    )
    log: list = []
    context.set_result(log)
    noisy_refusals = [0]
    handle_dispatch_execution = worker._handle_dispatch_execution

    async def faulty_dispatch_execution(dispatch, address, allocation_result) -> bytes:
        # The dispatch carries its workflow pickled by value: the class's
        # name is in the payload.
        is_noisy = SimNoisyWorkflow.__name__.encode() in dispatch.workflow
        if is_noisy and noisy_refusals[0] < refused_dispatches:
            noisy_refusals[0] += 1
            log.append(("noisy-dispatch-refused", noisy_refusals[0], round(context.loop.time(), 6)))
            raise RuntimeError(f"{SimNoisyWorkflow.__name__} cannot start on this worker")
        log.append(("dispatch-run", is_noisy, round(context.loop.time(), 6)))
        return await handle_dispatch_execution(dispatch, address, allocation_result)

    worker._handle_dispatch_execution = faulty_dispatch_execution

    async def run() -> None:
        await worker.start()
        log.append(("worker-started", round(context.loop.time(), 6)))

    context.loop.create_task(run())


def job_control_client_entry(
    context,
    host,
    port,
    target_tcp_addresses,
    workflow_kind,
    job_count,
    job_timeout_seconds,
    wait_timeout_seconds,
    target_tier="manager",
) -> None:
    """Client child: submit ``job_count`` jobs of the ``workflow_kind``
    class (``WORKFLOW_CLASSES``), each once the one before ended, to the
    managers -- or, ``target_tier`` "gate", the gates -- at
    ``target_tcp_addresses``. Also logs ``("job-ended-at", ordinal,
    status, accepting host, t)``: the host that took the job."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        **{f"{target_tier}s": list(target_tcp_addresses)},
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    workflow_class = WORKFLOW_CLASSES[workflow_kind]

    async def submit_and_await(ordinal: int) -> None:
        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], workflow_class())],
                    vus=2,
                    timeout_seconds=job_timeout_seconds,
                )
            except Exception as submit_error:
                log.append(("submit-rejected", ordinal, type(submit_error).__name__, round(context.loop.time(), 6)))
        log.append(("job-accepted", ordinal, round(context.loop.time(), 6)))
        result = await client.wait_for_job(job_id, timeout=wait_timeout_seconds)
        log.append(("job-finished", ordinal, result.status, round(context.loop.time(), 6)))
        accepting_host = client._state.get_job_target(job_id)[0]
        log.append(("job-ended-at", ordinal, result.status, accepting_host, round(context.loop.time(), 6)))

    async def run() -> None:
        await client.start()
        for ordinal in range(job_count):
            await submit_and_await(ordinal)

    context.loop.create_task(run())


def job_control_gate_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_managers,
    datacenter_manager_udp,
) -> None:
    """Gate child fronting ``datacenter_managers``. Logs ``("gate-job",
    status, t)`` whenever the status of a job it holds changes -- one job
    at a time in these scenarios -- ``("routed", primaries, fallbacks,
    health, t)`` for every placement it selects, ``("held", t)`` each time
    no datacenter had room for its job, and ``("gate-started", t)``."""
    gate = GateServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        datacenter_managers=datacenter_managers,
        datacenter_manager_udp=datacenter_manager_udp,
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    coordinator = gate._dispatch_coordinator
    select_datacenters = coordinator._select_datacenters
    hold_job_without_room = coordinator._hold_job_without_room

    async def observed_selection(count, preferred, job_id):
        primaries, fallbacks, health = await select_datacenters(count, preferred, job_id=job_id)
        log.append(("routed", tuple(primaries), tuple(fallbacks), health, round(context.loop.time(), 6)))
        return primaries, fallbacks, health

    async def observed_hold(job_id, room_refusals) -> None:
        log.append(("held", round(context.loop.time(), 6)))
        await hold_job_without_room(job_id, room_refusals)

    coordinator._select_datacenters = observed_selection
    coordinator._hold_job_without_room = observed_hold

    async def run() -> None:
        await gate.start()
        log.append(("gate-started", round(context.loop.time(), 6)))

    async def watch() -> None:
        last_statuses: tuple[str, ...] = ()
        while True:
            statuses = tuple(sorted(job.status for job in gate._job_manager._jobs.values()))
            if statuses != last_statuses:
                last_statuses = statuses
                for status in statuses:
                    log.append(("gate-job", status, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())
    context.loop.create_task(watch())
