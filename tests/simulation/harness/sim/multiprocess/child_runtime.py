"""
Child-process runtime for multi-process SIM.

``run_child_loop`` is the entry point every simulation child process
runs. It builds the process's ``SimulationLoop`` + ``CrossProcessTransport``
(+ ``VirtualClock`` / ``SeededRandom``, swapped in as the process
defaults), invokes the user ``entry`` (which registers endpoints and
schedules the process's initial behavior), then services the coordinator
over the pipe: each ``GRANT`` injects delivered events, drains the loop
to the window edge via ``SimulationLoop.run_window``, and reports the
next-event time plus this window's buffered outbound events, spawn
requests, and newly registered addresses back across the barrier.

Wire protocol (tuples, ``multiprocessing``-picklable):

- child -> coordinator, once after setup:
  ``("READY", addresses, next_event_time, outbound, spawn_requests)``
- coordinator -> child:
  ``("GRANT", deadline, inbound, process_events)`` | ``("STOP",)``
- child -> coordinator, per grant:
  ``("REPORT", next_event_time, outbound, spawn_requests, new_addresses)``
- child -> coordinator, at shutdown: ``("RESULT", result)``

where ``outbound`` items are ``(send_time, src_sockname, dst_addr,
payload)`` and ``inbound`` items are ``(delivery_time, dst_sockname,
src_addr, payload)`` — ``payload`` is a tagged wire event (datagram or
stream event; see ``child_context``), opaque to the coordinator;
``spawn_requests`` items are ``(process_id, entry, entry_args)`` —
requests for the coordinator to admit further child processes (the
``ProcessSpawner`` seam ``LocalServerPool`` drives under SIM);
``addresses`` / ``new_addresses`` are the socknames this process
registered since the previous barrier (servers may start mid-run, so
the route map grows incrementally); and ``process_events`` items are
``(kill_time, process_id, exitcode)`` — fault-injected deaths, made
visible to this process's exit-code snapshot at exactly ``kill_time``
so the production pool-health polling observes them on virtual time.
"""

import asyncio

from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from hyperscale.logging import LoggingConfig

from tests.simulation.harness.sim.seeded_random import SeededRandom
from tests.simulation.harness.sim.sim_filesystem import SimFilesystem
from tests.simulation.harness.sim.sim_system_resources import SimSystemResources
from tests.simulation.harness.sim.simulation_loop import SimulationLoop
from tests.simulation.harness.sim.virtual_clock import VirtualClock
from .child_context import ChildContext, CrossProcessTransport


def _audit_seam_bindings(virtual_clock, seeded_random, sim_filesystem) -> list:
    """Names of loaded production modules whose seam singletons are
    not the SIM instances — deferred imports that escaped the swaps."""
    import sys as _sys

    unswapped: list[str] = []
    for name, module in list(_sys.modules.items()):
        if module is None or not (
            name.startswith("hyperscale.distributed")
            or name.startswith("hyperscale.logging")
        ):
            continue
        clock = getattr(module, "_DEFAULT_CLOCK", None)
        if clock is not None and clock is not virtual_clock:
            unswapped.append(f"{name}._DEFAULT_CLOCK")
        random_source = getattr(module, "_DEFAULT_RANDOM", None)
        if random_source is not None and random_source is not seeded_random:
            unswapped.append(f"{name}._DEFAULT_RANDOM")
        filesystem = getattr(module, "_DEFAULT_FILESYSTEM", None)
        if filesystem is not None and filesystem is not sim_filesystem:
            unswapped.append(f"{name}._DEFAULT_FILESYSTEM")
    return sorted(unswapped)


def run_child_loop(
    conn,
    entry,
    entry_args,
    start_time: float = 0.0,
    seed: int = 1,
    initial_disk: dict | None = None,
) -> None:
    """Run one simulation child until the coordinator sends ``STOP``.

    ``conn`` is this child's end of a ``multiprocessing`` duplex pipe.
    ``entry`` is a top-level (picklable) callable ``entry(ctx, *entry_args)``
    that sets up the process's servers/behavior on ``ctx``. ``start_time``
    is 0.0 for children present at simulation start; a child admitted
    mid-run (a spawn request from another child) begins its virtual clock
    at the coordinator's global virtual time so its messages can never be
    timestamped in the global past. ``seed`` drives this process's
    ``SeededRandom`` (the coordinator derives a distinct, deterministic
    seed per child).
    """
    # The async Logger's stream setup uses connect_write_pipe /
    # run_in_executor, both banned on the SimulationLoop; SIM asserts on
    # state, not log output. Disable logging for the whole child process.
    LoggingConfig().disable()

    loop = SimulationLoop(start_time=start_time)
    asyncio.set_event_loop(loop)
    transport = CrossProcessTransport(loop)
    virtual_clock = VirtualClock(loop)
    seeded_random = SeededRandom(seed)
    # Clock-wired so the slow_disk fault can charge virtual time.
    sim_filesystem = SimFilesystem(clock=virtual_clock)
    if initial_disk is not None:
        # This generation rebooted over a prior generation's disk: the
        # durable state that survived its power loss.
        sim_filesystem.restore_durable(initial_disk)
    context = ChildContext(
        loop, transport, virtual_clock, seeded_random, sim_filesystem
    )

    # The multi-process twin of ``SimulationRuntime``'s default swap:
    # production modules that read the process-default ``Clock`` /
    # ``Random`` singletons (rather than an injected seam) get the
    # virtual clock and the seeded random, so jittered timers and
    # timeout bookkeeping inside this child are deterministic. The
    # entry's module graph is fully imported by the time we run (spawn
    # unpickled ``entry`` during bootstrap), so the swap covers it.
    defaults_snapshot = snapshot_defaults()
    sim_system_resources = SimSystemResources()
    swap_defaults(
        clock=virtual_clock,
        random_source=seeded_random,
        filesystem=sim_filesystem,
        # Constant machine telemetry: real psutil reads drift with host
        # load (earlier runs included) and rode into registration
        # payloads, diverging replay twins.
        system_resources=sim_system_resources,
    )

    entry(context, *entry_args)

    # Second swap pass: entry construction may FIRST-import production
    # modules (deferred function-level imports) whose seam singletons
    # were born AFTER the pre-entry swap and therefore still bind the
    # REAL clock/random/filesystem/telemetry — silent wall coupling
    # that diverges replay twins under host load. Rebinding again
    # covers every module the construction pulled in; the exit audit
    # below catches anything a REQUEST path defers even later.
    swap_defaults(
        clock=virtual_clock,
        random_source=seeded_random,
        filesystem=sim_filesystem,
        system_resources=sim_system_resources,
    )

    # Drain setup work at the joining instant, then announce readiness
    # with the addresses this process hosts (the coordinator's routing
    # map) and any children the setup itself requested.
    next_event_time = loop.run_window(start_time)
    conn.send(
        (
            "READY",
            transport.drain_new_addresses(),
            next_event_time,
            transport.drain_outbound(),
            transport.drain_spawn_requests(),
        )
    )

    try:
        while True:
            message = conn.recv()
            tag = message[0]
            if tag == "SNAPSHOT":
                # Coordinator-driven restart: power loss NOW. Arm the
                # reordering-crash fault if the event carries a seed,
                # collapse to durable, hand the surviving disk (and
                # this generation's result) back, and exit.
                fsync_reorder_seed = message[1]
                if fsync_reorder_seed is not None:
                    sim_filesystem.set_fsync_reorder(fsync_reorder_seed)
                sim_filesystem.crash()
                conn.send(
                    (
                        "SNAPSHOT_RESULT",
                        sim_filesystem.dump_durable(),
                        context.result,
                    )
                )
                break
            if tag == "STOP":
                unswapped = _audit_seam_bindings(
                    virtual_clock, seeded_random, sim_filesystem
                )
                result = context.result
                if unswapped:
                    # LOUD: a deferred import escaped both swap passes —
                    # its wall-bound defaults are a live nondeterminism
                    # source. Surfacing it in the result makes replay
                    # twins and scenario assertions fail WITH THE MODULE
                    # NAMED instead of diverging mysteriously.
                    result = (
                        list(result) if isinstance(result, list) else [result]
                    )
                    result.append(("determinism-audit-unswapped", unswapped))
                conn.send(("RESULT", result))
                return

            _, deadline, inbound, process_events = message
            for delivery_time, dst_sockname, src_addr, payload in inbound:
                transport.inject(delivery_time, dst_sockname, src_addr, payload)
            for kill_time, process_id, exitcode in process_events:
                # Recorded as a timer so the death becomes visible to
                # exit-code polling at exactly its virtual instant, not
                # at the window edge where the grant arrived.
                loop.call_at(
                    kill_time, transport.record_process_exit, process_id, exitcode
                )

            next_event_time = loop.run_window(deadline)
            conn.send(
                (
                    "REPORT",
                    next_event_time,
                    transport.drain_outbound(),
                    transport.drain_spawn_requests(),
                    transport.drain_new_addresses(),
                )
            )
    finally:
        _cancel_pending_tasks(loop)
        restore_defaults(defaults_snapshot)
        if not loop.is_closed():
            loop.close()
        asyncio.set_event_loop(None)


def _cancel_pending_tasks(loop) -> None:
    """Tear down like ``asyncio.run``: cancel every task still pending and
    deliver the cancellations before the loop closes.

    Closing the loop with tasks pending makes the garbage collector throw
    ``GeneratorExit`` into each suspended coroutine. Cleanup on the way
    out (``Queue.get`` cancelling its getter, a transport closing) then
    touches the closed loop and raises an ordinary ``RuntimeError`` that
    MASKS the ``GeneratorExit``; a background loop catching ``Exception``
    retries, its next sleep raises again at once, and the child spins
    forever instead of exiting (measured: a gate's windowed-stats loop,
    over a million iterations, wedging every later coordinator step).
    ``CancelledError`` is what production shutdown delivers, and no
    ``except Exception`` catches it. One window at the CURRENT virtual
    instant runs the cancellations without advancing time.
    """
    if loop.is_closed():
        return
    pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
    if not pending:
        return
    for task in pending:
        task.cancel()
    loop.run_window(loop.time())
