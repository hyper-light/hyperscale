"""
Structured Node Identifier for multi-datacenter SWIM clusters.

A node's identity is TOPOLOGY-DERIVED: ``(datacenter, priority, host,
port)``. It is a pure function of the node's configured placement — no
wall clock and no randomness enter identity or its ordering. Two
consequences, both load-bearing:

1. **Replay determinism.** Two runs of the same topology produce
   byte-identical identities, so leadership order and the cross-gate job
   hash-ring (both keyed on identity) are reproducible. The earlier
   ``uuid.uuid4()`` random component and the wall-clock ``created_ms``
   made every run's identities different, so any tie-break that touched
   identity diverged run-to-run — the defect this design removes at the
   root rather than papering over with a seeded RNG.

2. **Topology stability.** A node at a given address always has the same
   identity, leadership rank, and hash position, so restarting it neither
   reshuffles leadership nor remaps jobs.

Restart-distinguishability and false-suspicion refutation are NOT this
identity's job — they are owned by the dedicated, persisted
``IncarnationTracker`` / ``IncarnationStore`` (SWIM Lifeguard incarnation
numbers, keyed by ``host:port``). ``created_ms`` is retained ONLY as
human-readable observability metadata: it appears in the string form for
debugging but is excluded from equality, hashing, and ordering, so it can
never perturb identity or leadership.

Format: ``{datacenter}-{priority:02d}-{host}-{port:05d}-{created_ms:013x}``
Example: ``DC-EAST-01-10.0.0.4-09000-0018a3b2c4d5e``

- ``datacenter``, ``priority``, ``host``, ``port`` — the ORDERED identity
  (compared, hashed, sorted). ``port`` is zero-padded so the string form
  sorts consistently with the tuple order.
- ``created_ms`` — trailing metadata, ignored by ``==`` / ``hash`` / ``<``.

``priority`` is the intentional leadership knob (00-99, lower = higher
priority); ``host``/``port`` are the deterministic tie-break beneath it.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from hyperscale.distributed.runtime import Clock, RealClock

from .node_id_model import _DEFAULT_CLOCK
from .node_address import NodeAddress
from .node_id_model import NodeId

_REHOMED = (
    NodeId,
    NodeAddress,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
