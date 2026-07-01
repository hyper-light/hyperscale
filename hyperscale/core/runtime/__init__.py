"""
Runtime interfaces — the dependency-free home of the dependency-injection
seam Protocols consumed across layers.

Why this package exists
-----------------------

``hyperscale.core`` is the bottom layer: ``hyperscale.distributed`` is
built on top of it. But the SIM-mode seam Protocols (``TransportFactory``,
``ProcessSpawner``, ``SimulationChildContext``) are consumed by *core*
modules (``UDPProtocol``, ``LocalServerPool``, ``RemoteGraphManager``)
as well as by distributed ones. Housing them in
``hyperscale.distributed.runtime`` inverted the layering and forced every
core consumer into ``TYPE_CHECKING``-only imports with quoted
annotations to dodge a core -> distributed import cycle.

This package holds *interfaces only* — it imports nothing from the rest
of ``hyperscale``, so any layer can import it at runtime with no cycle
possible. Implementations stay where they belong: REAL defaults and the
swap machinery in ``hyperscale.distributed.runtime`` (which re-exports
these Protocols for its existing consumers), SIM implementations under
``tests/simulation/``.
"""

from .process_spawner import ProcessSpawner as ProcessSpawner
from .simulation_child_context import (
    SimulationChildContext as SimulationChildContext,
)
from .transport_factory import TransportFactory as TransportFactory
