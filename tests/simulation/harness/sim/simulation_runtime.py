"""
SimulationRuntime — the SIM-mode orchestrator.

The in-process equivalent of the REAL-mode ``Supervisor``: instead of
spawning server subprocesses and allocating OS ports, it stands up the
whole SIM stack in one process and hands servers the dependency-injection
seams that make them deterministic and socket-free.

On construction it:

1. Builds a ``SimulationLoop`` and installs it as the current event loop.
2. Builds a ``VirtualClock`` (loop-backed time) and a ``SeededRandom``.
3. Builds an ``InProcessTransport`` + ``SimTransportFactory`` for byte
   transit with no real sockets.
4. ``swap_defaults`` — rebinds every module-level ``_DEFAULT_CLOCK`` /
   ``_DEFAULT_RANDOM`` singleton under ``hyperscale.distributed`` to the
   SIM instances, so submodules that read the process default (rather
   than an injected seam) are deterministic too.
5. Disables logging — the async ``Logger`` sets up its output stream via
   ``connect_write_pipe`` and reads the cwd via ``run_in_executor``, both
   banned under the ``SimulationLoop``. SIM asserts on server state, not
   log output, so the kill-switch is the right trade.

Servers are constructed by the caller with ``**runtime.sim_kwargs()``
spread into their ``__init__`` (``clock`` / ``random_source`` /
``transport_factory``); the runtime stays node-type-agnostic. Drive the
loop with ``run(coro)`` and always ``close()`` to restore the process
defaults, re-enable logging, and tear the loop down.
"""

import asyncio

from hyperscale.logging import LoggingConfig
from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)

from .in_process_transport import InProcessTransport
from .seeded_random import SeededRandom
from .sim_filesystem import SimFilesystem
from .sim_transport_factory import SimTransportFactory
from .simulation_loop import SimulationLoop
from .virtual_clock import VirtualClock


class SimulationRuntime:
    """Own the SIM event loop + deterministic dependencies for one run.

    Construct, build servers with ``**sim_kwargs()``, ``run`` a
    scenario coroutine, then ``close``. Not reentrant — one runtime per
    scenario.
    """

    def __init__(self, seed: int = 1) -> None:
        self.loop = SimulationLoop()
        asyncio.set_event_loop(self.loop)
        self.clock = VirtualClock(self.loop)
        self.random = SeededRandom(seed)
        self.transport = InProcessTransport(self.loop)
        self.transport_factory = SimTransportFactory(self.transport)
        self.filesystem = SimFilesystem(clock=self.clock)

        # Snapshot the process-default clock/random so ``close`` can
        # restore them; then point them at the SIM instances.
        self._defaults = snapshot_defaults()
        swap_defaults(
            clock=self.clock,
            random_source=self.random,
            filesystem=self.filesystem,
        )

        # The SimulationLoop bans the real-I/O ops the async logger's
        # stream setup performs; SIM asserts on state, not log output.
        LoggingConfig().disable()

    def sim_kwargs(self) -> dict:
        """The DI kwargs to spread into a server's ``__init__``."""
        return {
            "clock": self.clock,
            "random_source": self.random,
            "transport_factory": self.transport_factory,
        }

    def run(self, coro):
        """Run ``coro`` to completion on the SIM loop and return its result."""
        return self.loop.run_until_complete(coro)

    def close(self) -> None:
        """Restore process defaults, re-enable logging, tear down the loop.

        Safe to call once; idempotent-friendly guards keep a double
        ``close`` from raising.
        """
        LoggingConfig().enable()
        restore_defaults(self._defaults)
        if not self.loop.is_closed():
            self.loop.close()
        asyncio.set_event_loop(None)
