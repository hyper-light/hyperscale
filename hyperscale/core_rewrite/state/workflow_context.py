import asyncio
from typing import Any, Dict


class WorkflowContext:
    def __init__(self) -> None:
        self._context: Dict[str, Any] = {}
        self._write_lock = asyncio.Lock()

    def __str__(self) -> str:
        return str(self._context)

    def __repr__(self) -> str:
        return str(self._context)

    def get(self, key: str, default: Any = None):
        return self._context.get(key, default)

    def __getitem__(self, key: str):
        return self._context[key]

    def dict(self):
        return self._context

    async def set(self, key: str, value: Any):
        await self._write_lock.acquire()
        self._context[key] = value
        self._write_lock.release()

    def items(self):
        return self._context.items()
