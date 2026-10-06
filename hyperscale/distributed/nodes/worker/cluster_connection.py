"""
Worker cluster-connection lifecycle.

The ``WorkerClusterConnection`` component owns the invariant *the
worker must remain connected to ≥1 live manager*, where "live" means
both (a) tracked as healthy in the registry (SWIM-level) and (b)
heard from at the application layer within the staleness window.

The two-axis liveness check is load-bearing for the kill-and-restart
case. When a manager is killed and a fresh process restarts at the
same UDP address, SWIM probes against that address keep succeeding
— but the new process has a different ``node_id`` and an empty
worker registry, so it does not send heartbeats to the worker that
was registered with the previous process. SWIM-level health alone
will leave the worker stuck pointing at a stale manager identity
forever. The application-level heartbeat-freshness signal closes the
gap: no heartbeat in ``HEARTBEAT_STALENESS_THRESHOLD`` seconds → the
manager is effectively unreachable from our perspective, regardless
of what SWIM thinks of its UDP address.

State machine:

  CONNECTING  ──first live manager────>  CONNECTED
       │                                     │
       │                                     │ last live manager goes stale
       │                                     ▼
       └──first live manager────────  RECONNECTING (rejoin task active)

Live-set derivation runs on two cadences:

* Synchronously, via ``update()``, called by every site that mutates
  ``_healthy_manager_ids`` (registry helpers, ``_on_peer_confirmed``,
  reap loop). This catches SWIM-driven changes immediately.
* Periodically, via the liveness watchdog (``_liveness_watchdog``)
  which scans ``_manager_last_heartbeat`` and marks managers that
  have gone heartbeat-stale as unhealthy in the registry, which in
  turn triggers ``update()``. This catches the application-level
  isolation (kill-and-restart) case that SWIM cannot see.

The rejoin task in ``RECONNECTING`` iterates the static seed manager
TCP addresses, attempts ``_register_with_manager(addr)`` against
each, and backs off with ``LHM``-scaled delay between full passes.
The task exits when the next iteration observes ``effective_live > 0``
— which can happen because (a) one of our register calls succeeded
and the response handler added a healthy manager + recorded its
heartbeat, or (b) some other path (SWIM gossip) added one and a
real heartbeat arrived. Either way the state machine converges; the
rejoin task is fire-and-forget bounded by state.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from enum import Enum
from typing import TYPE_CHECKING, Awaitable, Callable
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning
from hyperscale.distributed.runtime import Clock, RealClock, Random, RealRandom

from .worker_cluster_connection import _DEFAULT_CLOCK
from .worker_cluster_connection import _DEFAULT_RANDOM
from .cluster_connection_state import ClusterConnectionState
from .worker_cluster_connection import WorkerClusterConnection

_REHOMED = (
    ClusterConnectionState,
    WorkerClusterConnection,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
