"""
Runtime — the dependency-injection seams for time, randomness, and
transport in the distributed runtime.

Production code consumes the Protocols (``Clock``, ``Random``,
``Transport``) and accepts the ``RealClock`` / ``RealRandom``
implementations as defaults. Phase 6 SIM mode swaps in
``VirtualClock``, ``SeededRandom``, and ``InProcessTransport`` (all
living under ``tests/simulation/``) without any production-code
change.

Exit criterion of Phase 5: no production module under
``hyperscale/distributed/`` calls ``time.monotonic`` / ``time.time``
/ ``asyncio.sleep`` / ``asyncio.wait_for`` / non-crypto ``random.X``
directly — every such call routes through a ``Clock`` or ``Random``
on ``self``. The guard test under
``tests/simulation/lints/test_no_direct_time_random.py`` enforces the
boundary in CI.
"""

# The cross-layer seam Protocols live in ``hyperscale.core.runtime``
# (the dependency-free interface layer — see its package docstring) and
# are re-exported here so distributed-side consumers keep one import
# home for everything runtime-related. ``Filesystem`` / ``RealFilesystem``
# (Phase 7 storage seam) live there too because ``hyperscale.logging``
# — a bottom-layer package — consumes them as well.
from hyperscale.core.runtime import FileHandle as FileHandle
from hyperscale.core.runtime import Filesystem as Filesystem
from hyperscale.core.runtime import ProcessSpawner as ProcessSpawner
from hyperscale.core.runtime import RealFilesystem as RealFilesystem
from hyperscale.core.runtime import SystemResources as SystemResources
from hyperscale.core.runtime import (
    RealSystemResources as RealSystemResources,
)
from hyperscale.core.runtime import (
    SimulationChildContext as SimulationChildContext,
)
from hyperscale.core.runtime import TransportFactory as TransportFactory

from .clock import Clock as Clock
from .random_source import Random as Random
from .real_clock import RealClock as RealClock
from .real_random import RealRandom as RealRandom
from .runner import Runner as Runner
from .swap import restore_defaults as restore_defaults
from .swap import snapshot_defaults as snapshot_defaults
from .swap import swap_defaults as swap_defaults
from .transport import Transport as Transport
