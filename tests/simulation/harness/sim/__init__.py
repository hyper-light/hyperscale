"""
Phase 6 simulator primitives.

Every external source of asyncio non-determinism (real I/O,
thread/process executors, OS signals, real sockets, hash
randomization) is either replaced by a deterministic in-process
substitute under this package or banned at the ``SimulationLoop``
level. Production code under ``hyperscale/distributed/`` is
unchanged; the Phase 5 ``Clock`` / ``Random`` / ``Transport``
seams swap their backing implementations to the SIM versions here.

Public exports stay minimal so callers go through documented
classes rather than reaching into helpers.
"""

from .event_trace import EventTrace, TraceEntry
from .fake_tcp_transport import FakeTCPTransport
from .fake_udp_transport import FakeUDPTransport
from .in_process_transport import InProcessTransport
from .seeded_random import SeededRandom
from .sim_filesystem import SimFilesystem
from .sim_transport_factory import SimTransportFactory
from .simulation_constraint_error import SimulationConstraintError
from .simulation_loop import SimulationLoop
from .simulation_runtime import SimulationRuntime
from .virtual_clock import VirtualClock


__all__ = [
    "EventTrace",
    "FakeTCPTransport",
    "FakeUDPTransport",
    "InProcessTransport",
    "SeededRandom",
    "SimTransportFactory",
    "SimulationConstraintError",
    "SimulationLoop",
    "SimulationRuntime",
    "TraceEntry",
    "VirtualClock",
]
