from __future__ import annotations

import asyncio
import importlib
import importlib.util
import inspect
import itertools
import os
import pathlib
import re
import sys
import types
from typing import Any, Generic, TypeVar

from .reduce_pattern_type import reduce_pattern_type

T = TypeVar("T")

# Characters a Python identifier cannot hold.
NON_IDENTIFIER_CHARACTERS = re.compile(r"\W")


class ImportType(Generic[T]):
    def __init__(
        self,
        data_type: ImportType[T],
    ):
        super().__init__()
        self.data: dict[str, type[T]] | None = None

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
        """The classes of the requested types that the file -- or each
        Python file under the directory -- at ``arg`` defines or imports.
        Classes the process imported from anywhere else never join (an
        imported template Workflow once ran beside every test)."""
        if arg is None:
            return Exception("no argument passed for filepath")

        try:
            # Absolute with ".." collapsed, but symlinks kept: modules are
            # named by the path as given. A Kubernetes ConfigMap file
            # resolves into a hidden "..<timestamp>" directory, which is no
            # package name.
            given_path = pathlib.Path(await self._loop.run_in_executor(None, os.path.abspath, arg))
            sources = await self._loop.run_in_executor(None, _module_sources, given_path)
            return self._defined_types(await self._import_modules(sources), arg)

        except Exception as e:
            return Exception(f"could not import objects from file {arg}\n\t   {str(e)}")

    async def _import_modules(self, sources: list[tuple[str, pathlib.Path]]) -> list[types.ModuleType]:
        """Import each ``(module name, source path)`` in order."""
        return [await self._import_module(module_name, source_path) for module_name, source_path in sources]

    async def _import_module(self, module_name: str, source_path: pathlib.Path) -> types.ModuleType:
        """Import ``source_path`` as ``module_name``."""
        spec = await self._loop.run_in_executor(
            None,
            importlib.util.spec_from_file_location,
            module_name,
            source_path,
        )
        module = await self._loop.run_in_executor(None, importlib.util.module_from_spec, spec)
        sys.modules[module.__name__] = module
        await self._loop.run_in_executor(None, spec.loader.exec_module, module)
        return module

    def _defined_types(self, modules: list[types.ModuleType], arg: str) -> dict[str, type[T]] | Exception:
        """The requested types' subclasses that ``modules`` define or import
        by name, less any that another of them subclasses (a shared base is
        not run). Two different classes sharing a name, or none at all, is an
        error."""
        named = _unique_by_name(self._requested_members(modules))
        if isinstance(named, Exception):
            return Exception(f"{named} under {arg}")
        return _without_bases(named) or Exception(f"no {self.data_type} classes are defined in {arg}")

    def _requested_members(self, modules: list[types.ModuleType]) -> list[type[T]]:
        """The requested types' subclasses among ``modules``' names."""
        return [member for member in _members(modules) if self._is_requested(member)]

    def _is_requested(self, member: object) -> bool:
        """Whether ``member`` is a subclass of a requested type (the type
        itself excluded)."""
        return inspect.isclass(member) and any(_is_strict_subclass(member, requested) for requested in self._types)


def _members(modules: list[types.ModuleType]) -> list[object]:
    """Every name each module defines or imports."""
    return list(itertools.chain.from_iterable(vars(module).values() for module in modules))


def _is_strict_subclass(member: type, requested: type) -> bool:
    return member is not requested and issubclass(member, requested)


def _unique_by_name(classes: list[type]) -> dict[str, type] | Exception:
    """``classes`` by name; two different classes sharing a name is an error."""
    named: dict[str, type] = {}
    for candidate in classes:
        if named.setdefault(candidate.__name__, candidate) is not candidate:
            return Exception(f"two classes named {candidate.__name__}")
    return named


def _without_bases(named: dict[str, type]) -> dict[str, type]:
    """``named`` less every class another of them subclasses."""
    return {name: candidate for name, candidate in named.items() if not _is_base_of_another(candidate, named)}


def _is_base_of_another(candidate: type, named: dict[str, type]) -> bool:
    """Whether another collected class subclasses ``candidate``."""
    return any(other is not candidate and issubclass(other, candidate) for other in named.values())


def _module_sources(given_path: pathlib.Path) -> list[tuple[str, pathlib.Path]]:
    """Each module to import for ``given_path`` -- the file, or every
    Python file under the directory -- named under the directory's package
    as a file's always was, with that package's parent put on ``sys.path`` so
    its imports resolve."""
    package_root, sources = _package_and_sources(given_path)
    _add_to_sys_path(package_root.parent)
    return [(_module_name(package_root, source), source) for source in sources]


def _package_and_sources(given_path: pathlib.Path) -> tuple[pathlib.Path, list[pathlib.Path]]:
    if given_path.is_file():
        return given_path.parent, [given_path]
    if given_path.is_dir():
        return given_path, _python_sources(given_path)
    raise FileNotFoundError(f"{given_path} is neither a file nor a directory")


def _python_sources(directory: pathlib.Path) -> list[pathlib.Path]:
    """Every Python file under ``directory``, sorted; caches and hidden
    directories skipped."""
    return sorted(source for source in directory.rglob("*.py") if _is_importable(source.relative_to(directory)))


def _is_importable(relative_source: pathlib.Path) -> bool:
    return not any(part == "__pycache__" or part.startswith(".") for part in relative_source.parts)


def _add_to_sys_path(directory: pathlib.Path) -> None:
    if str(directory) not in sys.path:
        sys.path.append(str(directory))


def _module_name(package_root: pathlib.Path, source: pathlib.Path) -> str:
    """``source``'s dotted module name under ``package_root``'s package (a
    package's ``__init__.py`` is the package itself). Every part is made an
    identifier: a Workflow imports its own module by name, and a part such
    as "" (the filesystem root) or "..data" is no importable name."""
    parts = [_identifier(part) for part in (package_root.name, *source.relative_to(package_root).with_suffix("").parts)]
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def _identifier(name_part: str) -> str:
    """``name_part`` with each character an identifier cannot hold made
    "_", and "_" leading one that is empty or starts with a digit."""
    identifier = NON_IDENTIFIER_CHARACTERS.sub("_", name_part)
    return identifier if identifier.isidentifier() else f"_{identifier}"
