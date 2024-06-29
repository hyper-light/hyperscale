from __future__ import annotations

import asyncio
from typing import Any, Dict

from .workflow_context import WorkflowContext


class Context:
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

    def dict(self):
        return {key: value.dict() for key, value in self._context.items()}

    async def copy(self, context: Context):
        await asyncio.gather(
            *[
                self._context[workflow].set(key, value)
                for workflow, ctx in context.iter_workflow_contexts()
                for key, value in ctx.items()
            ]
        )

        return self

    async def update(
        self,
        workflow: str,
        key: str,
        value: Any,
        timestamp: int | None = None,
    ):
        if self._context.get(workflow) is None:
            self._context[workflow] = WorkflowContext()

        await self._context[workflow].set(
            key,
            value,
            timestamp=timestamp,
        )
