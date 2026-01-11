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

Supporting Classes (re-exported from hyperscale.distributed_rewrite.jobs):
- JobManager: Thread-safe job/workflow state management
- WorkerPool: Worker registration and resource allocation
- WorkflowDispatcher: Workflow dispatch and dependency tracking
- TrackingToken: Globally unique workflow tracking IDs
"""

from hyperscale.distributed_rewrite.nodes.worker import WorkerServer as WorkerServer
from hyperscale.distributed_rewrite.nodes.manager import ManagerServer as ManagerServer
from hyperscale.distributed_rewrite.nodes.gate.server import GateServer as GateServer
from hyperscale.distributed_rewrite.nodes.client import HyperscaleClient as HyperscaleClient

# Re-export supporting classes from jobs module for backwards compatibility
from hyperscale.distributed_rewrite.jobs import (
    JobManager as JobManager,
    JobInfo as JobInfo,
    WorkflowInfo as WorkflowInfo,
    SubWorkflowInfo as SubWorkflowInfo,
    TrackingToken as TrackingToken,
    WorkerPool as WorkerPool,
    WorkerInfo as WorkerInfo,
    WorkerHealth as WorkerHealth,
    WorkflowDispatcher as WorkflowDispatcher,
    WorkflowStateMachine as WorkflowStateMachine,
    CoreAllocator as CoreAllocator,
    AllocationResult as AllocationResult,
)

# Re-export PendingWorkflow from models
from hyperscale.distributed_rewrite.models import PendingWorkflow as PendingWorkflow
