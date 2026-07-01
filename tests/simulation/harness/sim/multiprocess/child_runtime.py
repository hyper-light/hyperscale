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

from ..seeded_random import SeededRandom
from ..simulation_loop import SimulationLoop
from ..virtual_clock import VirtualClock
from .child_context import ChildContext, CrossProcessTransport


def run_child_loop(
    conn, entry, entry_args, start_time: float = 0.0, seed: int = 1
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
    context = ChildContext(loop, transport, virtual_clock, seeded_random)

    # The multi-process twin of ``SimulationRuntime``'s default swap:
    # production modules that read the process-default ``Clock`` /
    # ``Random`` singletons (rather than an injected seam) get the
    # virtual clock and the seeded random, so jittered timers and
    # timeout bookkeeping inside this child are deterministic. The
    # entry's module graph is fully imported by the time we run (spawn
    # unpickled ``entry`` during bootstrap), so the swap covers it.
    defaults_snapshot = snapshot_defaults()
    swap_defaults(clock=virtual_clock, random_source=seeded_random)

    entry(context, *entry_args)

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
            if tag == "STOP":
                conn.send(("RESULT", context.result))
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
        restore_defaults(defaults_snapshot)
        if not loop.is_closed():
            loop.close()
        asyncio.set_event_loop(None)
