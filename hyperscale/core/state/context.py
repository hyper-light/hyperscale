from __future__ import annotations

import asyncio
from typing import Any, Dict

from .workflow_context import WorkflowContext


class Context:
    """
    Container for workflow contexts in a job.
    
    Maps workflow names to their WorkflowContext instances. Supports
    LWW (Last-Write-Wins) conflict resolution for distributed context sync.
    """
    
    def __init__(self) -> None:
        self._context: Dict[str, WorkflowContext] = {}

    def iter_workflow_contexts(self):
        for workflow, context in self._context.items():
            yield workflow, context

    def __str__(self) -> str:
        return str({key: str(value) for key, value in self._context.items()})

    def __repr__(self) -> str:
        return str({key: str(value) for key, value in self._context.items()})

    def __getitem__(self, key: str):
        result = self._context.get(key)

        if result is None:
            self._context[key] = WorkflowContext()

        return self._context[key]
    
    def __contains__(self, key: str) -> bool:
        return key in self._context

    def dict(self) -> Dict[str, Dict[str, Any]]:
        """Return the full context as nested dictionaries."""
        return {key: value.dict() for key, value in self._context.items()}
    
    def get_timestamps(self) -> Dict[str, Dict[str, int]]:
        """Return all timestamps for context sync."""
        return {
            workflow: ctx.get_timestamps() 
            for workflow, ctx in self._context.items()
        }
    
    async def from_dict(self, workflow: str, values: dict[str, Any]) -> "Context":
        """Load context values from a dictionary (no timestamps = always apply)."""
        if self._context.get(workflow) is None:
            self._context[workflow] = WorkflowContext()

        for key, value in values.items():
            await self._context[workflow].set(key, value)
        
        return self

    async def copy(self, context: Context):
        """Copy all values from another context (no timestamps = always apply)."""
        for workflow, ctx in context.iter_workflow_contexts():
            if self._context.get(workflow) is None:
                self._context[workflow] = WorkflowContext()
            for key, value in ctx.items():
                await self._context[workflow].set(key, value)

        return self

    async def update(
        self,
        workflow: str,
        key: str,
        value: Any,
        timestamp: int | None = None,
        source_node: str | None = None,
    ):
        """
        Update a context value with LWW conflict resolution.
        
        Args:
            workflow: The workflow name
            key: The context key
            value: The value to store
            timestamp: Lamport timestamp for ordering
            source_node: Node ID for tiebreaking (deterministic)
        """
        if self._context.get(workflow) is None:
            self._context[workflow] = WorkflowContext()

        await self._context[workflow].set(
            key,
            value,
            timestamp=timestamp,
            source_node=source_node,
        )
