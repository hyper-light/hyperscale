from __future__ import annotations

import asyncio
import importlib
import importlib.util
import ntpath
import pathlib
import sys
from typing import Any, Generic, TypeVar

from .reduce_pattern_type import reduce_pattern_type

T = TypeVar("T")


class ImportType(Generic[T]):
    def __init__(
        self,
        data_type: ImportType[T],
    ):
        super().__init__()
        self.data: dict[str, T] | None = None

        conversion_types: list[T] = reduce_pattern_type(data_type)

        self._data_types = [
            conversion_type.__name__
            if hasattr(conversion_type, "__name__")
            else type(conversion_type).__name__
            for conversion_type in conversion_types
        ]
        self._types = conversion_types

        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in [self._types]

    @property
    def data_type(self):
        return ", ".join(self._data_types)

    async def parse(self, arg: str | None = None):
        result = await self._import_types(arg)
        if isinstance(result, Exception):
            return result

        self.data = result

        return self

    async def _import_types(self, arg: str | None):
        try:
            if arg is None:
                return Exception("no argument passed for filepath")

            path = await self._loop.run_in_executor(None, pathlib.Path, arg)

            resolved_path = await self._loop.run_in_executor(None, path.resolve)

            package_dir = resolved_path.parent
            package_dir_path = str(package_dir)
            package_dir_module = package_dir_path.split("/")[-1]

            package = ntpath.basename(path)
            package_slug = package.split(".")[0]

            spec = await self._loop.run_in_executor(
                None,
                importlib.util.spec_from_file_location,
                f"{package_dir_module}.{package_slug}",
                arg,
            )

            if arg not in sys.path:
                await self._loop.run_in_executor(
                    None, sys.path.append, str(package_dir.parent)
                )

            module = await self._loop.run_in_executor(
                None,
                importlib.util.module_from_spec,
                spec,
            )

            sys.modules[module.__name__] = module

            await self._loop.run_in_executor(
                None,
                spec.loader.exec_module,
                module,
            )

            imported_types: dict[str, T] = {}

            for conversion_type in self._types:
                discovered = list(
                    {
                        cls.__name__: cls for cls in conversion_type.__subclasses__()
                    }.values()
                )

                imported_types.update({item.__name__: item for item in discovered})

            return imported_types

        except Exception as e:
            return Exception(f"could not import objects from file {arg}\n\t   {str(e)}")
