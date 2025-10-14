from __future__ import annotations

import asyncio
import functools
import operator
import os
import pathlib

from typing import Generic, Any, get_args, TypeVar
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar("T")


class Paths(Generic[T]):
    def __init__(
        self,
        data_type: Paths[T],
    ):
        super().__init__()

        self.data: list[str] | None = None

        args = get_args(data_type)

        search_patterns: list[T] = reduce_pattern_type(data_type)
        self._data_types = [
            search_pattern.__name__
            if hasattr(search_pattern, "__name__")
            else type(search_pattern).__name__
            for search_pattern in search_patterns
        ]

        self._patterns = search_patterns

        self._types = [type(pattern) for pattern in self._patterns]

        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in [self._types]

    @property
    def data_type(self):
        return ", ".join(self._data_types)

    async def parse(self, arg: str | None = None) -> T | Exception:
        result = await self._find_paths(arg)
        if isinstance(result, Exception):
            return result

        self.data = result

        return self

    async def _find_paths(self, arg: str | None):
        results: list[str] = []
        errors: list[Exception] = []

        matches = await asyncio.gather(
            *[
                self._find_path_with_pattern(
                    search_pattern,
                    arg,
                )
                for search_pattern in self._patterns
            ]
        )

        for match_set in matches:
            if not isinstance(match_set, Exception):
                results.extend(match_set)

            else:
                errors.append(match_set)

        if len(results) < 1 and len(errors) > 0:
            return errors[0]

        elif len(results) < 1 and len(errors) < 1:
            return Exception("no matches found")

        return results

    async def _find_path_with_pattern(
        self,
        search_pattern: str,
        arg: str,
    ):
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
                search_pattern,
            )

            matches: list[str] = [str(path) for path in path_matches]

            if len(matches) < 1:
                return Exception("no matches found")

            return matches

        except Exception as e:
            return Exception(
                f"encountered unexpected error {str(e)} searching pattern {search_pattern}"
            )
