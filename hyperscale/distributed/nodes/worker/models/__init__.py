"""
Worker-specific data models with slots for memory efficiency.

All state containers use dataclasses with slots=True per REFACTOR.md.
Shared protocol message models remain in distributed_rewrite/models/.
"""

from .workflow_runtime_state import WorkflowRuntimeState

__all__ = [
    "WorkflowRuntimeState",
]
