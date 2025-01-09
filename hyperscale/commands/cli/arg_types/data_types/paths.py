from __future__ import annotations

import asyncio
import os
import pathlib

from typing import Generic, TypeVar, Any
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar('T', bound=str)


class Paths(Generic[T]):

    def __init__(
        self,
        data_type: Paths[T],
    ):
        super().__init__()

        self.data: list[str] | None = None

        search_pattern: T = reduce_pattern_type(data_type)
        self._data_type = search_pattern.__name__ if hasattr(search_pattern, '__name__') else type(search_pattern).__name__

        self._pattern = search_pattern
        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in [self._pattern]

    @property
    def data_type(self):
        return self._pattern
    

    async def parse(self, arg: str | None = None) -> T | Exception:

        result = await self._find_paths(arg)
        if isinstance(result, Exception):
            return result
        
        self.data = result

        return self
    
    async def _find_paths(self, arg: str | None):

        search_pattern = self._pattern
        if search_pattern and arg:
            search_pattern = await self._loop.run_in_executor(
                None,
                os.path.join,
                search_pattern,
                arg,
            )

        elif search_pattern is None and arg is not None:
            search_pattern = arg
                
        try:
            path: pathlib.Path = await self._loop.run_in_executor(
                None,
                pathlib.Path,
            )

            path_matches = await self._loop.run_in_executor(
                None,
                path.rglob,
                self._pattern
            )

            matches: list[str] = [
                str(path) for path in path_matches
            ]

            if len(matches) < 1:
                return Exception('Err. - no matches found')
            
            return matches
            
            
        except Exception as e:
            return e
        