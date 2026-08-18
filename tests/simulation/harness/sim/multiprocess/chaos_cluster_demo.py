"""
CHAOS-suite child entries — the picklable children ``tests/simulation/
vopr_chaos`` drives (checklist E1-E4/H1 plus the A2/A4/A6/C6/F3/J2/K6/
L1 chaos vocabulary and the 0fabdab2 knob wave: pause, wire corruption,
read corruption, EIO, misdirected IO, wall-clock skew).

Three entries, each a superset of a committed shape so pinned schedules
elsewhere never move:

* ``chaos_manager_entry`` — gateless ``ManagerServer`` (WAL always on)
  with (a) the EXTENDED storage-fault vocabulary armed BOOT-AWARE (a
  generation rebooting inside a fault window recovers against the
  faulted disk from its first I/O — the B8 mechanism of
  ``recovery_faults_demo``), (b) a wall-clock skew schedule
  (``VirtualClock.set_wall_offset`` — the D1/D2 NTP-step model), and
  (c) the G3 node-side job milestones: acceptance and terminal
  transitions of the manager's OWN job table, index-keyed so the
  cross-node checker can compare them against client-observed
  terminals without ever logging a job id (the replay contract).
* ``chaos_multi_gate_manager_entry`` — the same manager attached
  upstream to a whole gate tier (the L3/multi-DC composition).
* ``chaos_multi_job_client_entry`` — a sequential multi-job client
  whose ``job<k>-`` milestone prefixes carry the FULL standard tag
  (``job3-job-finished``, not the soak entries' shortened
  ``job3-finished``), so ``tests/simulation/oracle/JobLogSplitter``
  splits and judges each stream directly, and whose per-job workflow
  CLASSES are freshly built with the job index baked into the class
  name (``SimChaosJob3Workflow``) — worker-side execution milestones
  (``dag_worker_entry``'s name-keyed rows) then attribute every
  attempt to its job, which is what makes K6 retry accounting and
  per-job placement judgments possible from the merged trace.

Extended storage-fault schedule vocabulary (all times absolute virtual
seconds; every windowed knob is armed boot-aware — past-due arming
happens synchronously during entry setup, strictly before the server
task's first step, exactly the ``recovery_faults_demo`` reasoning):

* ``("slow_disk", at, delay_seconds, until)`` — every storage op costs
  virtual time inside the window (B1, ride-through calibration);
* ``("disk_full_window", at, remaining_bytes, until)`` — ENOSPC beyond
  the byte budget inside the window, HEALED at ``until`` via
  ``clear_disk_full`` (unlike the permanent VOPR ``disk_full``, chaos
  windows must end by the quiesce instant — E2);
* ``("read_corruption", at, probability, knob_seed, until)`` — reads
  return seeded-flipped bytes (stored state intact) inside the window
  (B4: a CRC-failing read must be LOUD or a clean recovery-truncation);
* ``("io_error", at, probability, knob_seed, until)`` — transient
  EIO raises inside the window (B6: retried or loud, never silent).
  Armed ONCE at setup: ``SimFilesystem.set_io_error`` windows itself
  on the virtual clock, so re-arming at reboot is automatic;
* ``("misdirect", at, probability, knob_seed, until)`` — path-level
  writes/reads strike a seeded sibling file inside the window (B5:
  foreign bytes must be caught by framing/CRC, never applied).

Clock-skew schedule vocabulary: ``("wall_skew", at, delta_seconds)``
steps this node's WALL clock (``time()``) by an absolute offset at a
virtual instant; ``monotonic()`` and every timer are untouched (the
real NTP-step shape). Boot-aware: the latest step at-or-before a
generation's boot re-applies synchronously (a rebooted machine's RTC
is still skewed), and future steps re-arm on the virtual timeline.

Milestone vocabulary (values only — no node ids, no snowflakes, no
error text):

* manager: ``("manager-started", t)`` | ``("manager-start-failed",
  ExcName, t)``; ``("worker-count", n, t)`` per registry transition;
  ``("job<k>-accepted", t)`` when the k-th distinct job appears in
  this generation's job table (first-seen order — acceptance order);
  ``("job<k>-terminal", status, t)`` when that job's manager-side
  status first reads terminal (``JobStatusOrder`` is the spec).
  Indices restart per generation: a rebooted manager re-discovers its
  WAL-recovered jobs in replay order, which IS its acceptance order,
  so per-index comparisons stay meaningful across restarts while
  re-discovery rows never masquerade as duplicate acceptance.
* client: ``("job<k>-submit-rejected", ExcName, t)``,
  ``("job<k>-job-submitted", t)``, ``("job<k>-status-seen", status,
  t)``, ``("job<k>-wait-timed-out", t)`` (expiry is loud, then the
  entry re-waits UNBOUNDED so a late terminal still lands),
  ``("job<k>-job-finished", status, t)``, and ``("client-error",
  ExcName, t)`` before any unexpected re-raise.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname; the per-job workflow classes are
function-local and cloudpickled BY VALUE across the submission path,
reproducing the production user-script shape.
"""

import asyncio
import os
import sys
from pathlib import Path

import cloudpickle

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.jobs.job_status_order import JobStatusOrder
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.graph import Workflow, step

_AUTH_SECRET = "sim-multiprocess-secret-00000000"

# By-value pickling for everything this module defines — the manager's
# restricted unpickler admits hyperscale.* by reference only, so a
# tests-tree workflow class must travel by value (the production
# user-script shape).
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def apply_chaos_storage_fault_schedule(context, storage_fault_schedule) -> None:
    """Arm the extended storage-knob vocabulary on this child's
    ``SimFilesystem``, boot-aware (see the module docstring for the
    entry shapes and the per-knob WHYs).

    Boot-aware means: a windowed knob whose window straddles this
    generation's boot instant arms SYNCHRONOUSLY during entry setup —
    strictly before the server task's first step — so a generation
    rebooting INSIDE a fault window recovers against the faulted disk
    from its very first I/O (``recovery_faults_demo`` proved the
    ``call_at`` path fires only after the synchronous recovery prefix).
    Windows entirely in this generation's past are skipped whole.
    """
    filesystem = context.filesystem
    boot_time = context.loop.time()
    for event in storage_fault_schedule:
        kind = event[0]
        if kind == "slow_disk":
            _kind, at_time, delay_seconds, until_time = event
            _arm_window(
                context,
                boot_time,
                at_time,
                until_time,
                lambda delay=delay_seconds: filesystem.set_slow_disk(delay),
                filesystem.clear_slow_disk,
            )
        elif kind == "disk_full_window":
            _kind, at_time, remaining_bytes, until_time = event
            _arm_window(
                context,
                boot_time,
                at_time,
                until_time,
                lambda budget=remaining_bytes: filesystem.set_disk_full(budget),
                filesystem.clear_disk_full,
            )
        elif kind == "read_corruption":
            _kind, at_time, probability, knob_seed, until_time = event
            _arm_window(
                context,
                boot_time,
                at_time,
                until_time,
                lambda seed=knob_seed, chance=probability: (
                    filesystem.set_read_corruption(seed=seed, probability=chance)
                ),
                filesystem.clear_read_corruption,
            )
        elif kind == "io_error":
            _kind, at_time, probability, knob_seed, until_time = event
            # Self-windowing on the virtual clock: arm once per
            # generation at setup; the filesystem draws only inside
            # [at, until), so reboots re-arm automatically and no
            # call_at bracketing is needed.
            filesystem.set_io_error(
                seed=knob_seed,
                probability=probability,
                at_time=at_time,
                until_time=until_time,
            )
        elif kind == "misdirect":
            _kind, at_time, probability, knob_seed, until_time = event
            _arm_window(
                context,
                boot_time,
                at_time,
                until_time,
                lambda seed=knob_seed, chance=probability: (
                    filesystem.set_misdirect(seed=seed, probability=chance)
                ),
                filesystem.clear_misdirect,
            )
        else:
            raise ValueError(f"unknown chaos storage fault kind: {kind}")


def _arm_window(context, boot_time, at_time, until_time, arm, disarm) -> None:
    """Boot-aware set/clear bracketing for one windowed knob."""
    if until_time <= boot_time:
        return  # the window is entirely in this generation's past
    if at_time <= boot_time:
        arm()  # mid-window boot: faulted from the first recovery I/O
    else:
        context.loop.call_at(at_time, arm)
    context.loop.call_at(until_time, disarm)


def apply_clock_skew_schedule(context, clock_skew_schedule) -> None:
    """Arm ``("wall_skew", at, delta)`` steps on this child's
    ``VirtualClock``, boot-aware: the latest step at-or-before boot
    re-applies synchronously (a rebooted machine's RTC carries the
    skew), future steps arm on the virtual timeline. Offsets are
    absolute (each step models one NTP step to a definite skew)."""
    boot_time = context.loop.time()
    latest_past_delta: float | None = None
    for event in clock_skew_schedule:
        _kind, at_time, delta_seconds = event
        if at_time <= boot_time:
            latest_past_delta = delta_seconds
        else:
            context.loop.call_at(
                at_time, context.clock.set_wall_offset, delta_seconds
            )
    if latest_past_delta is not None:
        context.clock.set_wall_offset(latest_past_delta)


def _start_manager_watchers(context, manager, log: list) -> None:
    """The shared chaos-manager watcher set: start/start-failed,
    worker-registry count transitions, and the G3 job-lifecycle rows
    (acceptance + terminal per first-seen job index, 0.5s cadence)."""

    async def run() -> None:
        try:
            await manager.start()
        except Exception as start_error:
            # Storage faults during recovery can legitimately make
            # ``start()`` raise — the loud degraded path, never silence.
            log.append(
                (
                    "manager-start-failed",
                    type(start_error).__name__,
                    round(context.loop.time(), 6),
                )
            )
            return
        log.append(("manager-started", round(context.loop.time(), 6)))

    async def watch_worker_count() -> None:
        last_count = -1
        while True:
            count = manager._manager_state.get_worker_count()
            if count != last_count:
                last_count = count
                log.append(
                    ("worker-count", count, round(context.loop.time(), 6))
                )
            await asyncio.sleep(0.5)

    async def watch_job_lifecycle() -> None:
        status_order = JobStatusOrder()
        index_by_token: dict[str, int] = {}
        terminal_logged: set[int] = set()
        while True:
            for job_token in list(manager._job_manager._jobs.keys()):
                job_index = index_by_token.get(job_token)
                if job_index is None:
                    job_index = len(index_by_token) + 1
                    index_by_token[job_token] = job_index
                    log.append(
                        (
                            f"job{job_index}-accepted",
                            round(context.loop.time(), 6),
                        )
                    )
                if job_index in terminal_logged:
                    continue
                job_info = manager._job_manager._jobs.get(job_token)
                if job_info is None:
                    continue
                if status_order.is_terminal(job_info.status):
                    terminal_logged.add(job_index)
                    log.append(
                        (
                            f"job{job_index}-terminal",
                            job_info.status,
                            round(context.loop.time(), 6),
                        )
                    )
            await asyncio.sleep(0.5)

    context.loop.create_task(run())
    context.loop.create_task(watch_worker_count())
    context.loop.create_task(watch_job_lifecycle())


def chaos_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    storage_fault_schedule=(),
    clock_skew_schedule=(),
) -> None:
    """Gateless chaos manager: a real ``ManagerServer`` (WAL always on)
    with the extended storage/clock schedules armed boot-aware and the
    G3 worker-count + job-lifecycle watchers (module docstring)."""
    apply_chaos_storage_fault_schedule(context, storage_fault_schedule)
    apply_clock_skew_schedule(context, clock_skew_schedule)
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
    _start_manager_watchers(context, manager, log)


def chaos_multi_gate_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    gate_tcp_addresses,
    gate_udp_addresses,
    storage_fault_schedule=(),
    clock_skew_schedule=(),
) -> None:
    """Gate-attached chaos manager: ``chaos_manager_entry`` registered
    upstream with a whole gate tier (the L3 / multi-DC composition)."""
    apply_chaos_storage_fault_schedule(context, storage_fault_schedule)
    apply_clock_skew_schedule(context, clock_skew_schedule)
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id=datacenter_id,
        gate_addrs=gate_tcp_addresses,
        gate_udp_addrs=gate_udp_addresses,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    _start_manager_watchers(context, manager, log)


def _build_chaos_workflow(
    job_index: int, workflow_duration_seconds: float, workflow_vus: int
) -> type[Workflow]:
    """A fresh two-step ACTION-chain workflow class for one chaos job,
    the job index baked into the CLASS NAME (``SimChaosJob3Workflow``)
    so worker-side name-keyed execution milestones attribute every
    attempt to its job. ACTION sleeps, not TEST duration governance —
    the committed SIM constraint (``WorkflowRunner._generate``
    busy-waits frozen virtual time; ``l2_workload_demo`` documents the
    probe). The class ``duration`` matches the summed sleeps so
    worker-side bookkeeping windows agree."""
    step_sleep_seconds = workflow_duration_seconds / 2.0

    class SimChaosWorkflow(Workflow):
        vus = workflow_vus
        duration = f"{workflow_duration_seconds:g}s"
        timeout = f"{workflow_duration_seconds + 30.0:g}s"

        @step()
        async def chaos_leg_one(self) -> dict[str, str]:
            await asyncio.sleep(step_sleep_seconds)
            return {"leg": "one"}

        @step("chaos_leg_one")
        async def chaos_leg_two(self) -> dict[str, str]:
            await asyncio.sleep(step_sleep_seconds)
            return {"leg": "two"}

    SimChaosWorkflow.__name__ = f"SimChaosJob{job_index}Workflow"
    SimChaosWorkflow.__qualname__ = SimChaosWorkflow.__name__
    return SimChaosWorkflow


def chaos_multi_job_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    submit_times,
    durations,
    job_timeout_seconds,
    wait_timeout_seconds,
    workflow_vus=2,
) -> None:
    """Client child: submit ``len(submit_times)`` STRICTLY SEQUENTIAL
    chaos jobs directly to a manager (gateless L2), each a fresh
    index-named workflow class, and await each to its terminal.

    Job ``k`` submits at ``max(submit_times[k-1], previous job's
    terminal)`` — at most one job is in flight, so the occupancy
    schedule stays deterministic under any recovery latency, and a
    fault mid-horizon intersects a KNOWN job. A ``wait_for_job``
    expiry logs ``job<k>-wait-timed-out`` and the entry then waits
    UNBOUNDED: on a doomed schedule (permanent manager loss — J2) the
    queue blocks there LOUDLY and later jobs legitimately never
    submit; the invariants branch on the plan flavor. The entry (and
    its client) stays alive to the ceiling so every push has a live
    destination (the dead-client orphan path is deliberately out of
    this suite's space).
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

    async def submit_and_await(job_index: int, workflow_duration: float) -> None:
        workflow_class = _build_chaos_workflow(
            job_index, workflow_duration, workflow_vus
        )
        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], workflow_class())],
                    vus=workflow_vus,
                    timeout_seconds=job_timeout_seconds,
                )
            except Exception as submit_error:
                # Production rejects until it is leader with capacity
                # (and through every chaos window) — retry on virtual
                # time. Type name only: texts embed per-run values.
                log.append(
                    (
                        f"job{job_index}-submit-rejected",
                        type(submit_error).__name__,
                        round(context.loop.time(), 6),
                    )
                )
                await asyncio.sleep(1.0)

        log.append(
            (f"job{job_index}-job-submitted", round(context.loop.time(), 6))
        )

        async def watch_status() -> None:
            last_status: str | None = None
            while True:
                job_result = client.get_job_status(job_id)
                status = job_result.status if job_result is not None else None
                if status != last_status:
                    last_status = status
                    log.append(
                        (
                            f"job{job_index}-status-seen",
                            status,
                            round(context.loop.time(), 6),
                        )
                    )
                await asyncio.sleep(0.5)

        status_watcher = context.loop.create_task(watch_status())
        try:
            result = await client.wait_for_job(
                job_id, timeout=wait_timeout_seconds
            )
        except asyncio.TimeoutError:
            # LOUD, then keep waiting: the ceiling bounds the run and a
            # late terminal must still reach the log.
            log.append(
                (
                    f"job{job_index}-wait-timed-out",
                    round(context.loop.time(), 6),
                )
            )
            result = await client.wait_for_job(job_id)
        finally:
            # Guarded: at teardown the coordinator STOP can close the
            # loop while ``run`` is parked on the wait; cancelling
            # against a closed loop raises inside generator close.
            if not context.loop.is_closed():
                status_watcher.cancel()
        log.append(
            (
                f"job{job_index}-job-finished",
                result.status,
                round(context.loop.time(), 6),
            )
        )

    async def run() -> None:
        try:
            await client.start()
            for job_index, submit_at in enumerate(submit_times, 1):
                now = context.loop.time()
                if now < submit_at:
                    await asyncio.sleep(submit_at - now)
                await submit_and_await(job_index, durations[job_index - 1])
        except asyncio.CancelledError:
            raise
        except Exception as client_error:
            # Child logging is disabled under SIM: record the failure
            # type loudly (stable across replays), then re-raise —
            # never swallow.
            log.append(
                (
                    "client-error",
                    type(client_error).__name__,
                    round(context.loop.time(), 6),
                )
            )
            raise

    context.loop.create_task(run())
