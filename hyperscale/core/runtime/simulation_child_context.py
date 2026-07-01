"""
SimulationChildContext interface — what a multi-process SIM child entry
receives as its first argument.

A child entry (for example ``run_sim_executor`` in
``hyperscale/core/jobs/runner/local_server_pool.py``) is a top-level
picklable callable invoked as ``entry(context, *entry_args)`` inside a
freshly spawned coordinator child process. The context carries the two
injection points every production server needs under SIM — the
process's ``SimulationLoop`` and its cross-process transport factory —
plus the spawn seam so a child can itself request further children
(a worker node spawning its executor pool). The concrete
implementation is the harness ``ChildContext`` under
``tests/simulation/``; production code only consumes this Protocol.
"""

import asyncio
from typing import Callable, Protocol

from .transport_factory import TransportFactory


class SimulationChildContext(Protocol):
    """The per-process handle a SIM child entry configures itself on."""

    loop: asyncio.AbstractEventLoop
    transport: TransportFactory

    def spawn_process(
        self,
        process_id: str,
        entry: Callable[..., None],
        *entry_args,
    ) -> None:
        """Request a further coordinator child (see ``ProcessSpawner``)."""
        ...

    def set_result(self, value) -> None:
        """Record the value returned to the coordinator at shutdown."""
        ...
