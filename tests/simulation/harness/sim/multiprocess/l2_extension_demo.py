"""
The AD-26 EXTENSION-OBSERVING manager entry — a real ``ManagerServer``
whose watcher milestones expose the extension grant/refuse surface on
virtual time (the K5 scenario family's data source).

Mechanism being observed (traced through the production code):

* The worker fires ``request_extension(reason="dispatch-accepted")``
  the moment a workflow dispatch with ``timeout_seconds > 0`` is
  accepted (``WorkerServer._handle_dispatch_execution``), latching
  ``WorkerState._extension_requested``. ``clear_extension_request``
  has NO callers anywhere in the tree, so the latch never clears:
  every subsequent SWIM heartbeat re-carries the SAME dispatch-time
  snapshot, and the autonomous lookahead ``ExtensionTrigger``
  (``elapsed >= deadline x 0.75``) is permanently gated by its
  ``is_extension_pending`` check — dead code in practice. The
  scenarios pin what ACTUALLY runs: the heartbeat-piggyback path
  through ``_process_extension_request_core``.
* The manager processes that piggyback per heartbeat:
  ``ExtensionTracker.request_extension`` grants the first request
  (30s at ``extension_count=0``; logarithmic decay would follow) and
  DENIES every repeat (the latched snapshot's ``completed_items``
  never advances), recording every decision in the H7
  ``ExtensionLedger`` and adding granted seconds to the job's AD-34
  ``effective_timeout`` (``record_worker_extension``).

Milestones (``(tag, value..., virtual_time)`` only — no node ids, no
snowflakes, no error text):

* ``("manager-started", t)`` / ``("worker-registered", t)`` /
  ``("worker-lost", t)`` — the ``manager_entry``-compatible lifecycle
  milestones.
* ``("ext-grants", total, t)`` — transition of the summed
  ``ExtensionTracker.extension_count`` across workers (grants are
  monotone per tracker).
* ``("ext-denials", total, t)`` — transition of the summed
  consecutive-failure counters (reset to 0 by a grant; the deny
  streak's shape is part of the pinned schedule).
* ``("ext-extended", seconds, t)`` — transition of the summed
  ``total_extended`` grant seconds.
* ``("ext-last-code", code, t)`` — transition of the most recent
  ledger decision's ``denial_reason_code`` (``"none"`` on grant).
* ``("job-ext", seconds, t)`` — transition of the summed
  ``TimeoutTrackingState.total_extensions_granted`` across live jobs:
  the AD-34 effective-timeout stretch the grants actually bought.

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname.
"""

import asyncio
from pathlib import Path
import os

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.manager.server import ManagerServer

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def extension_watch_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
) -> None:
    """Manager child: start a real ``ManagerServer`` (WAL on, exactly
    as ``manager_entry``) and watch the AD-26 extension surface.

    The extension watcher samples every 0.5s and logs transitions
    only, so the log pins the decision SCHEDULE (first-grant instant,
    deny-streak growth, effective-timeout stretch) without recording
    per-heartbeat noise beyond actual state changes.
    """
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
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

        while manager._manager_state.get_worker_count() < 1:
            await asyncio.sleep(0.5)
        log.append(("worker-registered", round(context.loop.time(), 6)))

        while manager._manager_state.get_worker_count() > 0:
            await asyncio.sleep(0.5)
        log.append(("worker-lost", round(context.loop.time(), 6)))

    async def watch_extensions() -> None:
        health_manager = manager._worker_health_manager
        last_grants = -1
        last_denials = -1
        last_extended = -1.0
        last_code: str | None = None
        last_job_extension = -1.0
        while True:
            grants = sum(
                tracker.extension_count
                for tracker in health_manager._trackers.values()
            )
            if grants != last_grants:
                last_grants = grants
                log.append(
                    ("ext-grants", grants, round(context.loop.time(), 6))
                )

            denials = sum(health_manager._extension_failures.values())
            if denials != last_denials:
                last_denials = denials
                log.append(
                    ("ext-denials", denials, round(context.loop.time(), 6))
                )

            extended = round(
                sum(
                    tracker.total_extended
                    for tracker in health_manager._trackers.values()
                ),
                3,
            )
            if extended != last_extended:
                last_extended = extended
                log.append(
                    ("ext-extended", extended, round(context.loop.time(), 6))
                )

            latest_decision_code: str | None = None
            latest_decision_time = -1.0
            for ledger_entry in health_manager.ledger.iter_active_workflows():
                decision_event = ledger_entry.last_decision
                if (
                    decision_event is not None
                    and decision_event.timestamp > latest_decision_time
                ):
                    latest_decision_time = decision_event.timestamp
                    latest_decision_code = decision_event.denial_reason_code
            if latest_decision_code is not None and (
                latest_decision_code != last_code
            ):
                last_code = latest_decision_code
                log.append(
                    (
                        "ext-last-code",
                        latest_decision_code,
                        round(context.loop.time(), 6),
                    )
                )

            job_extension = round(
                sum(
                    job.timeout_tracking.total_extensions_granted
                    for job in manager._job_manager.iter_jobs()
                    if job.timeout_tracking is not None
                ),
                3,
            )
            if job_extension != last_job_extension:
                last_job_extension = job_extension
                log.append(
                    (
                        "job-ext",
                        job_extension,
                        round(context.loop.time(), 6),
                    )
                )

            await asyncio.sleep(0.5)

    context.loop.create_task(run())
    context.loop.create_task(watch_extensions())
