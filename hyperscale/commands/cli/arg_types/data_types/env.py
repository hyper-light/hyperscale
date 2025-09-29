from __future__ import annotations

import asyncio
import os
from typing import Generic, TypeVar, Any
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar("T")


class Env(Generic[T]):
    def __init__(
        self,
        envar: str,
        data_type: Env[T],
    ):
        super().__init__()
        self._envar = envar

        self.data: T | None = None

        data_types = reduce_pattern_type(data_type)
        self._types = data_types

        self._data_types = [
            conversion_type.__name__
            if hasattr(conversion_type, "__name__")
            else type(conversion_type).__name__
            for conversion_type in self._types
        ]

        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in [self._types]

    @property
    def data_type(self):
        return self._data_types

    async def parse(self, arg: str | None = None):
        
        if arg:
            self.data = arg

            return self

        result = await self._load_envar()
        if isinstance(result, Exception):
            return result

        self.data = result

        return self

    async def _load_envar(self):
        value = await self._load()
        if value is None:
            return Exception(f"no envar matching {self._envar.upper()} found")

        for conversion_type in self._types:
            try:
                if conversion_type == bytes:
                    return bytes(value, encoding="utf-8")

                else:
                    return conversion_type(value)

            except Exception:
                pass

        return Exception(f"could not parse {value} specified types")

    async def _load(self):
        value = await self._loop.run_in_executor(
            None,
            os.getenv,
            self._envar.upper(),
        )

        return value
