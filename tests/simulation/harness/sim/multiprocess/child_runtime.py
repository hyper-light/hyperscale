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
  ``("READY", addresses, next_event_time, outbound)``
- coordinator -> child: ``("GRANT", deadline, inbound)`` | ``("STOP",)``
- child -> coordinator, per grant: ``("REPORT", next_event_time, outbound)``
- child -> coordinator, at shutdown: ``("RESULT", result)``

where ``outbound`` items are ``(send_time, src_sockname, dst_addr, data)``
and ``inbound`` items are ``(delivery_time, dst_sockname, src_addr, data)``.
"""

import asyncio

from hyperscale.logging import LoggingConfig

from ..simulation_loop import SimulationLoop
from .child_context import ChildContext, CrossProcessTransport


def run_child_loop(conn, entry, entry_args) -> None:
    """Run one simulation child until the coordinator sends ``STOP``.

    ``conn`` is this child's end of a ``multiprocessing`` duplex pipe.
    ``entry`` is a top-level (picklable) callable ``entry(ctx, *entry_args)``
    that sets up the process's servers/behavior on ``ctx``.
    """
    # The async Logger's stream setup uses connect_write_pipe /
    # run_in_executor, both banned on the SimulationLoop; SIM asserts on
    # state, not log output. Disable logging for the whole child process.
    LoggingConfig().disable()

    loop = SimulationLoop()
    asyncio.set_event_loop(loop)
    transport = CrossProcessTransport(loop)
    context = ChildContext(loop, transport)

    entry(context, *entry_args)

    # Drain any t=0 setup work, then announce readiness with the
    # addresses this process hosts (the coordinator's routing map).
    next_event_time = loop.run_window(0.0)
    conn.send(("READY", transport.addresses(), next_event_time, transport.drain_outbound()))

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
            conn.send(("REPORT", next_event_time, transport.drain_outbound()))
    finally:
        if not loop.is_closed():
            loop.close()
        asyncio.set_event_loop(None)
