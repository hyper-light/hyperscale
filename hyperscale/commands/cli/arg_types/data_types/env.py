from __future__ import annotations

import asyncio
import os
from typing import Generic, TypeVar, Any
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar('T')


class Env(Generic[T]):
    
    def  __init__(
        self,
        envar: str,
        data_type: Env[T],
    ):
        super().__init__()
        self._envar = envar

        self.data: T | None = None

        data_type = reduce_pattern_type(data_type)
        self._type = data_type

        self._data_type = data_type.__name__ if hasattr(data_type, '__name__') else type(data_type).__name__

        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in [self._type]

    @property
    def data_type(self):
        return self._data_type
    
    async def parse(self, _: str | None = None):
        
        result = await self._load_envar()
        if isinstance(result, Exception):
            return result
        
        self.data = result

        return self
    
    async def _load_envar(self):

        value = await self._load()

        try:
            if self._type == bytes:
                return bytes(value, encoding='utf-8')
            
            else:
                return self._type(value)
        
        except Exception as e:
            return e

    async def _load(self):
        value = await self._loop.run_in_executor(
            None,
            os.getenv,
            self._envar.upper(),
        )

        return value