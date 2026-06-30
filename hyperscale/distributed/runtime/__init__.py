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

from .clock import Clock as Clock
from .random_source import Random as Random
from .real_clock import RealClock as RealClock
from .real_random import RealRandom as RealRandom
from .swap import restore_defaults as restore_defaults
from .swap import snapshot_defaults as snapshot_defaults
from .swap import swap_defaults as swap_defaults
from .transport import Transport as Transport
