"""
Distributed node implementations for Hyperscale.

This module provides Gate, Manager, Worker, and Client implementations.
Servers inherit from the SWIM HealthAwareServer for UDP healthchecks
and add TCP handlers for their specific role.

Architecture:
- Worker: Executes workflows, reports to managers via TCP
- Manager: Orchestrates workers within a DC, handles quorum
- Gate: Coordinates across DCs, manages leases
- Client: Submits jobs and receives push notifications

Supporting Classes:
- JobManager: Thread-safe job/workflow state management
- WorkerPool: Worker registration and resource allocation
- WorkflowDispatcher: Workflow dispatch and dependency tracking
- TrackingToken: Globally unique workflow tracking IDs
"""

from .worker import WorkerServer as WorkerServer
from .manager import ManagerServer as ManagerServer
from .gate import GateServer as GateServer
from .client import HyperscaleClient as HyperscaleClient

# Supporting classes for manager
from .job_manager import (
    JobManager as JobManager,
    JobInfo as JobInfo,
    WorkflowInfo as WorkflowInfo,
    SubWorkflowInfo as SubWorkflowInfo,
    WorkflowState as WorkflowState,
    TrackingToken as TrackingToken,
)
from .worker_pool import (
    WorkerPool as WorkerPool,
    WorkerInfo as WorkerInfo,
    WorkerHealth as WorkerHealth,
)
from .workflow_dispatcher import (
    WorkflowDispatcher as WorkflowDispatcher,
    PendingWorkflow as PendingWorkflow,
    DispatchPriority as DispatchPriority,
)

