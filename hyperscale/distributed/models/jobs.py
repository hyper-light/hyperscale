"""
Job-related models for internal manager tracking.

These models are used by the manager's job tracking system for
internal state management. They are not wire protocol messages.

Tracking Token Format:
======================
All workflow tracking uses globally unique tokens with the format:

    <DATACENTER>:<MANAGER_NODE_ID>:<JOB_ID>:<WORKFLOW_ID>:<WORKER_NODE_ID>

Components:
- DATACENTER: Datacenter/region identifier (e.g., "DC-EAST")
- MANAGER_NODE_ID: Short node ID of the manager that owns the job
- JOB_ID: Unique job identifier
- WORKFLOW_ID: Unique workflow identifier within the job
- WORKER_NODE_ID: Short node ID of the worker (for sub-workflows only)

Examples:
- Job token:      DC-EAST:mgr-abc123:job-def456
- Workflow token: DC-EAST:mgr-abc123:job-def456:wf-001
- Sub-workflow:   DC-EAST:mgr-abc123:job-def456:wf-001:wrk-xyz789

This module is the wire namespace of the models below. Each lives in a
file of its own and is re-homed here -- its ``__module__`` set to this
module -- so its pickled form names this module, exactly as before the
split: mixed-version clusters keep talking and data written earlier
keeps loading.
"""

from .job_info import JobInfo
from .pending_workflow import PendingWorkflow
from .sub_workflow_info import SubWorkflowInfo
from .timeout_tracking_state import TimeoutTrackingState
from .tracking_token import TrackingToken
from .workflow_info import WorkflowInfo

_WIRE_MODELS = (
    TrackingToken,
    WorkflowInfo,
    SubWorkflowInfo,
    TimeoutTrackingState,
    JobInfo,
    PendingWorkflow,
)

for _wire_model in _WIRE_MODELS:
    _wire_model.__module__ = __name__
