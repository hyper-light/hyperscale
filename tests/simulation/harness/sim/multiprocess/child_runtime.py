"""
Child-process runtime for multi-process SIM.

``run_child_loop`` is the entry point every simulation child process
runs. It builds the process's ``SimulationLoop`` + ``CrossProcessTransport``,
invokes the user ``entry`` (which registers endpoints and schedules the
process's initial behavior), then services the coordinator over the pipe:
each ``GRANT`` injects delivered datagrams, drains the loop to the window
edge via ``SimulationLoop.run_window``, and reports the next-event time
plus this window's buffered outbound datagrams back across the barrier.

Wire protocol (tuples, ``multiprocessing``-picklable):

- child -> coordinator, once after setup:
  ``("READY", addresses, next_event_time, outbound, spawn_requests)``
- coordinator -> child: ``("GRANT", deadline, inbound)`` | ``("STOP",)``
- child -> coordinator, per grant:
  ``("REPORT", next_event_time, outbound, spawn_requests)``
- child -> coordinator, at shutdown: ``("RESULT", result)``

where ``outbound`` items are ``(send_time, src_sockname, dst_addr, data)``,
``inbound`` items are ``(delivery_time, dst_sockname, src_addr, data)``,
and ``spawn_requests`` items are ``(process_id, entry, entry_args)`` —
requests for the coordinator to admit further child processes (the
``ProcessSpawner`` seam ``LocalServerPool`` drives under SIM).
"""

import asyncio

from hyperscale.logging import LoggingConfig

from ..simulation_loop import SimulationLoop
from .child_context import ChildContext, CrossProcessTransport


def run_child_loop(conn, entry, entry_args, start_time: float = 0.0) -> None:
    """Run one simulation child until the coordinator sends ``STOP``.

    ``conn`` is this child's end of a ``multiprocessing`` duplex pipe.
    ``entry`` is a top-level (picklable) callable ``entry(ctx, *entry_args)``
    that sets up the process's servers/behavior on ``ctx``. ``start_time``
    is 0.0 for children present at simulation start; a child admitted
    mid-run (a spawn request from another child) begins its virtual clock
    at the coordinator's global virtual time so its messages can never be
    timestamped in the global past.
    """
    # The async Logger's stream setup uses connect_write_pipe /
    # run_in_executor, both banned on the SimulationLoop; SIM asserts on
    # state, not log output. Disable logging for the whole child process.
    LoggingConfig().disable()

    loop = SimulationLoop(start_time=start_time)
    asyncio.set_event_loop(loop)
    transport = CrossProcessTransport(loop)
    context = ChildContext(loop, transport)

    entry(context, *entry_args)

    # Drain setup work at the joining instant, then announce readiness
    # with the addresses this process hosts (the coordinator's routing
    # map) and any children the setup itself requested.
    next_event_time = loop.run_window(start_time)
    conn.send(
        (
            "READY",
            transport.addresses(),
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

            _, deadline, inbound = message
            for delivery_time, dst_sockname, src_addr, data in inbound:
                transport.inject(delivery_time, dst_sockname, src_addr, data)

            next_event_time = loop.run_window(deadline)
            conn.send(
                (
                    "REPORT",
                    next_event_time,
                    transport.drain_outbound(),
                    transport.drain_spawn_requests(),
                )
            )
    finally:
        if not loop.is_closed():
            loop.close()
        asyncio.set_event_loop(None)
