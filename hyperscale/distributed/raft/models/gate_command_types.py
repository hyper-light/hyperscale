"""
Gate Raft command types for GateJobManager mutations.

Each value maps to exactly one state-mutating operation in
GateJobManager. Used by the gate state machine for deterministic dispatch.
"""

from enum import Enum


class GateRaftCommandType(str, Enum):
    """
    Command types for gate Raft log entries.

    Maps to GateJobManager's 20 state-mutating methods plus
    Raft control operations.
    """

    # Job CRUD (2 methods)
    SET_JOB = "set_job"
    DELETE_JOB = "delete_job"

    # Target datacenter management (2 methods)
    SET_TARGET_DCS = "set_target_dcs"
    ADD_TARGET_DC = "add_target_dc"

    # DC results (1 method)
    SET_DC_RESULT = "set_dc_result"

    # Callback management (2 methods)
    SET_CALLBACK = "set_callback"
    REMOVE_CALLBACK = "remove_callback"

    # Fence token management (1 method)
    SET_FENCE_TOKEN = "set_fence_token"

    # Cleanup (1 method)
    CLEANUP_OLD_JOBS = "cleanup_old_jobs"

    # Gate leadership (4 methods)
    ASSUME_GATE_LEADERSHIP = "assume_gate_leadership"
    TAKEOVER_GATE_LEADERSHIP = "takeover_gate_leadership"
    RELEASE_GATE_LEADERSHIP = "release_gate_leadership"
    PROCESS_LEADERSHIP_CLAIM = "process_leadership_claim"

    # DC manager tracking (2 methods)
    UPDATE_DC_MANAGER = "update_dc_manager"
    RELEASE_DC_MANAGERS = "release_dc_managers"

    # Lease management (2 methods)
    CREATE_LEASE = "create_lease"
    RELEASE_LEASE = "release_lease"

    # Job submission state (1 method)
    SET_JOB_SUBMISSION = "set_job_submission"

    # Workflow DC results (1 method)
    SET_WORKFLOW_DC_RESULT = "set_workflow_dc_result"

    # Membership events (1 method)
    GATE_MEMBERSHIP_EVENT = "gate_membership_event"

    # Raft control
    NO_OP = "no_op"
