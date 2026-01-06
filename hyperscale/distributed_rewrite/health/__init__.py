"""
Health model infrastructure for distributed nodes (AD-19).

Three-signal health model for all node types:
- Liveness: Is the node responding? (heartbeat-based)
- Readiness: Can the node accept work? (capacity-based)
- Progress: Is the node making progress? (throughput-based)

This module provides:
- WorkerHealthState: Manager monitors workers
- ManagerHealthState: Gate monitors managers
- GateHealthState: Gates monitor peer gates
- NodeHealthTracker: Generic health tracking infrastructure
- HealthPiggyback: Data structure for SWIM message embedding
"""

from hyperscale.distributed_rewrite.health.worker_health import (
    ProgressState as ProgressState,
    RoutingDecision as RoutingDecision,
    WorkerHealthConfig as WorkerHealthConfig,
    WorkerHealthState as WorkerHealthState,
)
from hyperscale.distributed_rewrite.health.manager_health import (
    ManagerHealthConfig as ManagerHealthConfig,
    ManagerHealthState as ManagerHealthState,
)
from hyperscale.distributed_rewrite.health.gate_health import (
    GateHealthConfig as GateHealthConfig,
    GateHealthState as GateHealthState,
)
from hyperscale.distributed_rewrite.health.tracker import (
    EvictionDecision as EvictionDecision,
    HealthPiggyback as HealthPiggyback,
    HealthSignals as HealthSignals,
    NodeHealthTracker as NodeHealthTracker,
    NodeHealthTrackerConfig as NodeHealthTrackerConfig,
)
from hyperscale.distributed_rewrite.health.extension_tracker import (
    ExtensionTracker as ExtensionTracker,
    ExtensionTrackerConfig as ExtensionTrackerConfig,
)
