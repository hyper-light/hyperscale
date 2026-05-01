"""
Simulation harness public surface.

Phase 1 deliverable: REAL-mode foundation. Process tracking, supervised
cleanup, port allocation, cluster construction. SIM-mode arrives in
Phases 5–6 once the Clock/Random/Transport refactor lands.
"""

from tests.simulation.harness.cluster_harness import ClusterHarness
from tests.simulation.harness.cluster_spec import ClusterSpec
from tests.simulation.harness.dc_spec import DCSpec
from tests.simulation.harness.env_overrides import EnvOverrides
from tests.simulation.harness.errors import (
    HarnessError,
    PortConflictError,
    ReapError,
    LeakedAsyncTasksError,
    PreflightZombieError,
)
from tests.simulation.harness.execution_mode import ExecutionMode
from tests.simulation.harness.port_allocator import PortAllocator
from tests.simulation.harness.server_handle import ServerHandle, ServerKind
from tests.simulation.harness.supervisor import Supervisor
from tests.simulation.harness.timeouts import HarnessTimeouts
from tests.simulation.harness.worker_ports import WorkerPorts


__all__ = [
    "ClusterHarness",
    "ClusterSpec",
    "DCSpec",
    "EnvOverrides",
    "ExecutionMode",
    "HarnessError",
    "HarnessTimeouts",
    "LeakedAsyncTasksError",
    "PortAllocator",
    "PortConflictError",
    "PreflightZombieError",
    "ReapError",
    "ServerHandle",
    "ServerKind",
    "Supervisor",
    "WorkerPorts",
]
