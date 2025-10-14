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


class ImportInstance(Generic[T]):
    def __init__(
        self,
        data_type: ImportInstance[T],
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
        result = await self._import_instance(arg)
        if isinstance(result, Exception):
            return result

        self.data = result

        return self

    async def _import_instance(self, arg: str):
        try:
            path = pathlib.Path(arg).absolute().resolve()
            path_str = str(path)

            package_dir = path.parent
            package_dir_path = str(package_dir)
            package_dir_module = package_dir_path.split("/")[-1]

            package = ntpath.basename(path)
            package_slug = package.split(".")[0]

            spec = importlib.util.spec_from_file_location(
                f"{package_dir_module}.{package_slug}", path_str
            )

            if path_str not in sys.path:
                sys.path.append(str(package_dir.parent))

            module = importlib.util.module_from_spec(spec)
            sys.modules[module.__name__] = module

            spec.loader.exec_module(module)

            imported_types: dict[str, T] = {}

            for conversion_type in self._types:
                imported_types.update(
                    {
                        name: value
                        for name, value in module.__dict__.items()
                        if isinstance(value, conversion_type)
                    }
                )

            return imported_types

        except Exception as e:
            return Exception(f"could not import objects from file {arg}\n\t   {str(e)}")
