import asyncio
import os
from typing import Generic, TypeVar, get_args, Any


T = TypeVar('T')


def reduce_pattern_type(pattern: Any):
    if isinstance(pattern, tuple) and len(pattern) > 0:
        for arg in pattern:
            return reduce_pattern_type(arg)
        
    elif (
        args := get_args(pattern)
    ) and len(args) > 0:
        return reduce_pattern_type(args)
    
    else:
        return pattern



class Env(Generic[T]):
    
    def  __init__(
        self,
        envar: str,
        data_type: type[T],
    ):
        super().__init__()
        self._envar = envar

        data_type = reduce_pattern_type(data_type)
        self._types = tuple([
            data_type
        ])

        self._data_type = [
            subtype_type.__name__ for subtype_type in self._types
        ]

        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in self._types

    @property
    def data_type(self):
        return ', '.join(self._data_type)
    
    async def parse(self):

        value = await self._load()

        parse_error: Exception | None = None

        for subtype in self._types:
            try:
                if subtype == bytes:
                    return bytes(value, encoding='utf-8')
                
                else:
                    return subtype(value)
            
            except Exception as e:
                parse_error = e

        return parse_error

    async def _load(self):
        value = await self._loop.run_in_executor(
            None,
            os.getenv,
            self._envar.upper(),
        )

        return value