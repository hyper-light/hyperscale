"""
Simulation harness public surface.

Phase 1 deliverable: REAL-mode foundation. Process tracking, supervised
cleanup, port allocation, cluster construction. SIM-mode arrives in
Phases 5–6 once the Clock/Random/Transport refactor lands.
"""

from tests.simulation.harness.cluster_harness import ClusterHarness
from tests.simulation.harness.cluster_spec import ClusterSpec
from tests.simulation.harness.conditions import (
    ConditionTimeoutError,
    WaitContext,
    all_of,
    any_of,
    gate_cluster_formed,
    manager_has_n_peers,
    manager_has_n_workers,
    wait_until,
    worker_subprocesses_alive,
)
from tests.simulation.harness.dc_spec import DCSpec
from tests.simulation.harness.diagnostics import DiagnosticDumper
from tests.simulation.harness.env_overrides import EnvOverrides
from tests.simulation.harness.errors import (
    HarnessError,
    LeakedAsyncTasksError,
    PortConflictError,
    PreflightZombieError,
    ReapError,
)
from tests.simulation.harness.execution_mode import ExecutionMode
from tests.simulation.harness.expectations import (
    ExpectAllWorkflowsComplete,
    ExpectCompletionWithin,
    Expectation,
    ExpectationResult,
    WorkloadObservations,
)
from tests.simulation.harness.invariants import (
    InvariantChecker,
    InvariantResult,
    InvariantViolation,
    LivenessInvariant,
    SafetyInvariant,
    Severity,
    at_most_one_job_leader_per_job,
    cluster_membership_progress,
)
from tests.simulation.harness.port_allocator import PortAllocator
from tests.simulation.harness.server_handle import ServerHandle, ServerKind
from tests.simulation.harness.submission import (
    Submission,
    SubmissionPattern,
    WorkflowFactory,
    WorkloadSpec,
)
from tests.simulation.harness.supervisor import Supervisor
from tests.simulation.harness.timeouts import HarnessTimeouts
from tests.simulation.harness.worker_ports import WorkerPorts
from tests.simulation.harness.workload import WorkloadDriver, WorkloadFailure


__all__ = [
    "ClusterHarness",
    "ClusterSpec",
    "ConditionTimeoutError",
    "DCSpec",
    "DiagnosticDumper",
    "EnvOverrides",
    "ExecutionMode",
    "ExpectAllWorkflowsComplete",
    "ExpectCompletionWithin",
    "Expectation",
    "ExpectationResult",
    "HarnessError",
    "HarnessTimeouts",
    "InvariantChecker",
    "InvariantResult",
    "InvariantViolation",
    "LeakedAsyncTasksError",
    "LivenessInvariant",
    "PortAllocator",
    "PortConflictError",
    "PreflightZombieError",
    "ReapError",
    "SafetyInvariant",
    "ServerHandle",
    "ServerKind",
    "Severity",
    "Submission",
    "SubmissionPattern",
    "Supervisor",
    "WaitContext",
    "WorkerPorts",
    "WorkflowFactory",
    "WorkloadDriver",
    "WorkloadFailure",
    "WorkloadObservations",
    "WorkloadSpec",
    "all_of",
    "any_of",
    "at_most_one_job_leader_per_job",
    "cluster_membership_progress",
    "gate_cluster_formed",
    "manager_has_n_peers",
    "manager_has_n_workers",
    "wait_until",
    "worker_subprocesses_alive",
]
