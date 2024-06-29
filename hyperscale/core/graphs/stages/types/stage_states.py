from enum import Enum


class StageStates(Enum):
    ACTING = "ACTING"
    ACTED = "ACTED"
    INITIALIZED = "INITIALIZED"
    SETTING_UP = "SETTING_UP"
    SETUP = "SETUP"
    VALIDATING = "VALIDATING"
    VALIDATED = "VALIDATED"
    OPTIMIZING = "OPTIMIZING"
    OPTIMIZED = "OPTIMIZED"
    EXECUTING = "EXECUTING"
    EXECUTED = "EXECUTED"
    ANALYZING = "ANALYZING"
    ANALYZED = "ANALYZED"
    CHECKPOINTING = "CHECKPOINTING"
    CHECKPOINTED = "CHECKPOINTED"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    TEARDOWN_INITIALIZED = "TEARDOWN_INITIALIZED"
    TEARDOWN_COMPLETE = "TEARDOWN_COMPLETE"
    COMPLETE = "COMPLETE"
    ERRORED = "ERRORED"
