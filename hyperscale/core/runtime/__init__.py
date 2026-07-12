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

This package imports nothing from the rest of ``hyperscale``, so any
layer can import it at runtime with no cycle possible. It holds the
seam *interfaces*, plus one deliberate exception to interfaces-only:
the stdlib-bound ``RealFilesystem`` default. ``hyperscale.logging`` is
itself a bottom-layer package (it must not import from
``hyperscale.distributed``) yet needs the production filesystem
implementation for its module default, so that REAL default lives here
beside its Protocol. Every other REAL default and the swap machinery
stay in ``hyperscale.distributed.runtime`` (which re-exports these
Protocols for its existing consumers); SIM implementations live under
``tests/simulation/``.
"""

from .filesystem import FileHandle as FileHandle
from .filesystem import Filesystem as Filesystem
from .process_spawner import ProcessSpawner as ProcessSpawner
from .real_filesystem import RealFileHandle as RealFileHandle
from .real_filesystem import RealFilesystem as RealFilesystem
from .simulation_child_context import (
    SimulationChildContext as SimulationChildContext,
)
from .transport_factory import TransportFactory as TransportFactory
