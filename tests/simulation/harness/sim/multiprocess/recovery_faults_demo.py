"""
Recovery-path fault scenarios (checklist B8 / C4 / C6) — the picklable
child entries the gateless L2 restart scenarios drive: a manager whose
storage-fault schedule is armed BOOT-AWARE (so a generation rebooting
inside a fault window recovers against the faulted disk from its first
I/O — see ``apply_boot_aware_storage_fault_schedule``), and a client
with a parameterized, LOUD wait deadline.

``job_dispatch_demo.dispatch_client_entry`` awaits ``wait_for_job``
with a fixed 45s deadline and NO timeout handling: any scenario whose
completion lands later (a manager restart with a 45s down window
already does) kills the waiting task with an unhandled TimeoutError and
loses the ``job-finished`` milestone. The recovery scenarios pin
completions that arrive after one or even two down windows, so this
entry uses the soak-entry pattern instead: a parameterized wait
deadline whose expiry is recorded LOUDLY as ``("wait-timed-out", t)``
followed by an unbounded re-wait — a stranded job stays visible in the
log rather than silently killing the watcher.

``submit_at`` delays the whole client lifecycle to a virtual instant,
so worker-restart scenarios can submit a job strictly AFTER the
rebooted generation is up (the C4 fresh-job pin).

Milestone vocabulary (values only — no node ids, no snowflakes; the
log is part of the byte-identical replay contract), a superset of
``job_dispatch_demo``'s and exactly ``soak_job_demo``'s:

* ``("submit-rejected", ExcName, t)`` — each refused submission
* ``("job-submitted", t)`` — acceptance
* ``("status-seen", status, t)`` — every observed status transition
* ``("wait-timed-out", t)`` — ``wait_for_job`` hit its deadline
* ``("job-finished", status, t)`` — result delivery (exactly once)

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname; ``SimPingWorkflow`` ships by value via
``job_dispatch_demo``'s ``register_pickle_by_value``.
"""

import asyncio
import os
from pathlib import Path

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.manager.server import ManagerServer

from .job_dispatch_demo import SimPingWorkflow

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env() -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET)


def apply_boot_aware_storage_fault_schedule(
    context, storage_fault_schedule
) -> None:
    """Arm ``SimFilesystem`` fault knobs so a rebooted generation's
    knobs are active BEFORE its first boot I/O.

    ``worker_manager_demo.apply_storage_fault_schedule`` arms every
    event via ``loop.call_at``. A rebooted generation replays the same
    entry args, and a past ``at_time`` timer is indeed due at the boot
    instant — but the server task created during entry setup sits AHEAD
    of the due-timer handle in the ready queue, and its first step runs
    the entire synchronous recovery prefix (incarnation store, WAL
    replay, resume) before ever suspending on a pending future. The
    past-due arming callback therefore fires only at the task's first
    real suspension — AFTER the recovery I/O it was meant to cover
    (probe-verified: gen-2 ``manager-started`` lands at exactly the
    boot instant under a 20ms slow disk armed the ``call_at`` way).

    A disk that was slow (or full) before the power loss is slow (or
    full) when the machine reboots, so past-due events here arm
    SYNCHRONOUSLY during entry setup — strictly before the server
    task's first step — and only genuinely future events use
    ``call_at``. Vocabulary matches ``apply_storage_fault_schedule``:
    ``("slow_disk", at_time, delay_seconds, until_time)`` and
    ``("disk_full", at_time, remaining_bytes)``.
    """
    filesystem = context.filesystem
    boot_time = context.loop.time()
    for event in storage_fault_schedule:
        kind = event[0]
        if kind == "slow_disk":
            _kind, at_time, delay_seconds, until_time = event
            if at_time <= boot_time:
                filesystem.set_slow_disk(delay_seconds)
            else:
                context.loop.call_at(
                    at_time, filesystem.set_slow_disk, delay_seconds
                )
            if until_time > boot_time:
                context.loop.call_at(until_time, filesystem.clear_slow_disk)
        elif kind == "disk_full":
            _kind, at_time, remaining_bytes = event
            if at_time <= boot_time:
                filesystem.set_disk_full(remaining_bytes)
            else:
                context.loop.call_at(
                    at_time, filesystem.set_disk_full, remaining_bytes
                )
        else:
            raise ValueError(f"unknown storage fault kind: {kind}")


def recovery_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    storage_fault_schedule=(),
) -> None:
    """Manager child for the recovery-fault scenarios: identical to
    ``worker_manager_demo.manager_entry`` (gateless, WAL always on)
    except the storage-fault schedule is armed boot-aware, so a
    generation rebooting INSIDE a fault window recovers against the
    faulted disk from its very first I/O — the B8 mechanism.

    Milestones (values only): ``("manager-started", t)`` after
    ``start()`` returns, ``("worker-registered", t)`` when the first
    worker lands in the registry, ``("worker-lost", t)`` if the
    registry later empties, and — because storage faults during
    recovery can legitimately make ``start()`` raise — a LOUD
    ``("manager-start-failed", ExcName, t)`` if it does (exception TYPE
    only: messages can embed per-run values).
    """
    apply_boot_aware_storage_fault_schedule(context, storage_fault_schedule)
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id=datacenter_id,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        try:
            await manager.start()
        except Exception as start_error:
            log.append(
                (
                    "manager-start-failed",
                    type(start_error).__name__,
                    round(context.loop.time(), 6),
                )
            )
            return
        log.append(("manager-started", round(context.loop.time(), 6)))

        while manager._manager_state.get_worker_count() < 1:
            await asyncio.sleep(0.5)
        log.append(("worker-registered", round(context.loop.time(), 6)))

        while manager._manager_state.get_worker_count() > 0:
            await asyncio.sleep(0.5)
        log.append(("worker-lost", round(context.loop.time(), 6)))

    context.loop.create_task(run())


def recovery_dispatch_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    wait_timeout_seconds,
    submit_at=0.0,
) -> None:
    """Client child: submit ``SimPingWorkflow`` directly to a manager
    (the gateless L2 topology) and await completion with a LOUD,
    parameterized wait deadline.

    * ``wait_timeout_seconds`` — the ``wait_for_job`` deadline; expiry
      logs ``("wait-timed-out", t)`` and the entry then waits UNBOUNDED
      so a terminal delivered after any number of down windows still
      lands in the log (the simulation ceiling bounds the run).
    * ``submit_at`` — virtual instant the client lifecycle starts
      (0.0 = immediately): worker-restart scenarios use it to submit
      strictly after the rebooted generation is back up.
    """
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await client.start()

        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], SimPingWorkflow())],
                    vus=2,
                    timeout_seconds=30.0,
                )
            except Exception as submit_error:
                # Production behavior: the manager rejects submissions
                # until it is leader with registered capacity — retry.
                # Exception TYPE only: rejection texts can embed
                # per-run values (node ids, addresses).
                log.append(
                    (
                        "submit-rejected",
                        type(submit_error).__name__,
                        round(context.loop.time(), 6),
                    )
                )
                await asyncio.sleep(1.0)

        log.append(("job-submitted", round(context.loop.time(), 6)))

        async def watch_status() -> None:
            last_status: str | None = None
            while True:
                job_result = client.get_job_status(job_id)
                status = job_result.status if job_result is not None else None
                if status != last_status:
                    last_status = status
                    log.append(
                        ("status-seen", status, round(context.loop.time(), 6))
                    )
                await asyncio.sleep(0.5)

        status_watcher = context.loop.create_task(watch_status())
        try:
            result = await client.wait_for_job(
                job_id, timeout=wait_timeout_seconds
            )
        except asyncio.TimeoutError:
            # LOUD, then keep waiting: the simulation ceiling bounds
            # the run, and a late terminal must still reach the log.
            log.append(("wait-timed-out", round(context.loop.time(), 6)))
            result = await client.wait_for_job(job_id)
        status_watcher.cancel()
        log.append(
            ("job-finished", result.status, round(context.loop.time(), 6))
        )

    if submit_at > 0.0:
        context.loop.call_at(submit_at, lambda: context.loop.create_task(run()))
    else:
        context.loop.create_task(run())
