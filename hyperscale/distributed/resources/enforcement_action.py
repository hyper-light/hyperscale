from enum import Enum


class EnforcementAction(Enum):
    """What an AD-41 resource check decided for one workflow."""

    NONE = "none"
    WARN = "warn"
    THROTTLE_WORKFLOW = "throttle_workflow"
    KILL_WORKFLOW = "kill_workflow"
    EVICT_WORKER = "evict_worker"
