"""
Raft command types for manager JobManager mutations.

Each value maps to exactly one state-mutating operation in
JobManager. Used by the state machine for deterministic dispatch.
"""

from enum import Enum


class RaftCommandType(str, Enum):
    """
    Command types for manager Raft log entries.

    Maps to JobManager's 21 state-mutating methods plus
    Raft control operations.
    """

    # Job lifecycle (3 methods)
    CREATE_JOB = "create_job"
    TRACK_REMOTE_JOB = "track_remote_job"
    COMPLETE_JOB = "complete_job"

    # Workflow registration (2 methods)
    REGISTER_WORKFLOW = "register_workflow"
    REGISTER_SUB_WORKFLOW = "register_sub_workflow"

    # Progress and results (2 methods)
    UPDATE_WORKFLOW_PROGRESS = "update_workflow_progress"
    RECORD_SUB_WORKFLOW_RESULT = "record_sub_workflow_result"

    # Workflow completion (4 methods)
    MARK_WORKFLOW_COMPLETED = "mark_workflow_completed"
    MARK_WORKFLOW_FAILED = "mark_workflow_failed"
    MARK_AGGREGATION_FAILED = "mark_aggregation_failed"
    UPDATE_WORKFLOW_STATUS = "update_workflow_status"

    # State management (2 methods)
    UPDATE_JOB_STATUS = "update_job_status"
    UPDATE_CONTEXT = "update_context"

    # Job leadership (3 methods)
    ASSUME_JOB_LEADERSHIP = "assume_job_leadership"
    TAKEOVER_JOB_LEADERSHIP = "takeover_job_leadership"
    RELEASE_JOB_LEADERSHIP = "release_job_leadership"

    # Cancellation (2 methods)
    INITIATE_CANCELLATION = "initiate_cancellation"
    COMPLETE_CANCELLATION = "complete_cancellation"

    # Provisioning (1 method)
    PROVISION_CONFIRMED = "provision_confirmed"

    # Stats (1 method)
    FLUSH_STATS_WINDOW = "flush_stats_window"

    # Membership (1 method)
    NODE_MEMBERSHIP_EVENT = "node_membership_event"

    # Raft control
    NO_OP = "no_op"
