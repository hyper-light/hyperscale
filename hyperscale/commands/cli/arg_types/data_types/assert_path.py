from __future__ import annotations

import asyncio
import os
import pathlib

from typing import Any, TypeVar


T = TypeVar("T")


class AssertPath:
    def __init__(self):
        super().__init__()

        self.data: str | None = None

        self._data_types = [str]

        self._types = [str]

        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in [self._types]

    @property
    def data_type(self):
        return ", ".join(self._data_types)

    async def parse(self, arg: str | None = None) -> T | Exception:
        path, valid = await self._loop.run_in_executor(
            None,
            self._validate_path,
            arg,
        )
        if isinstance(path, Exception):
            return path

        elif valid is False:
            return Exception(f'Path {arg} does not exist')

        self.data = path

        return self

    def _validate_path(self, arg: str | None):
        
        try:
            path = str(pathlib.Path(arg).absolute().resolve())

            if os.path.exists(path) is False:
                return path, False

            return path, True

        except Exception as e:
            return Exception(
                f"encountered unexpected error {str(e)} validating path {arg}"
            )