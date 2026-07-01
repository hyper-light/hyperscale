"""
Multi-process deterministic simulation.

Where the single-process ``InProcessTransport`` runs a whole cluster in
one ``SimulationLoop``, this package preserves the real multi-process
topology — every node and every worker-pool executor stays its own OS
process running its own ``SimulationLoop`` — while making the
*cross-process* message boundary itself replay-deterministic.

The mechanism is a conservative, barrier-synchronized lockstep
discrete-event simulation (Chandy-Misra-Bryant style with a fixed
positive lookahead):

- ``SimulationCoordinator`` (parent process) owns global virtual time
  and is the sole router of cross-process messages — UDP datagrams and
  TCP stream events (connect/accept/refuse/data/close) alike, carried
  as opaque payloads routed by destination sockname.
- Each child runs ``run_child_loop`` over a ``multiprocessing`` pipe:
  it drains its ``SimulationLoop`` up to the granted window edge,
  buffering outbound messages tagged with ``(send_time, seq)``, then
  reports ``(next_event_time, outbound[])`` and blocks at the barrier.
- The coordinator waits for every child (barrier), so it holds a
  complete snapshot; it schedules each message for delivery at
  ``send_time + latency`` in a global queue ordered by
  ``(delivery_time, origin, seq)`` — a total deterministic order — and
  advances global time to the minimum of all next-event times and the
  earliest pending delivery.

Because the fixed latency is strictly positive, a message sent in a
window is always delivered in a strictly later window (the lookahead),
so the barrier never has to reorder within an instant. The result
depends only on ``(virtual_time, origin, seq)`` ordering and is
independent of the wall-clock order in which pipe bytes happen to
arrive — same seed, byte-identical global schedule, every run.
"""

from .child_context import ChildContext, CrossProcessTransport
from .child_runtime import run_child_loop
from .simulation_coordinator import SimulationCoordinator


__all__ = [
    "ChildContext",
    "CrossProcessTransport",
    "SimulationCoordinator",
    "run_child_loop",
]
