from __future__ import annotations

import asyncio
from typing import Any, Callable, Generic, TypeVar, TypeVarTuple, get_args, get_origin

from hyperscale.commands.cli.arg_types.data_types import (
    AssertPath,
    AssertSet,
    Env,
    ImportInstance,
    ImportType,
    JsonData,
    JsonFile,
    Paths,
    Pattern,
    RawFile,
)

T = TypeVarTuple("T")
K = TypeVar("K")


class Map(Generic[*T]):
    def __init__(
        self,
        name: str,
        data_type: Map[tuple[*T]],
    ):
        super().__init__()

        self.name = name
        self._types: tuple[*T] = get_args(data_type)

        self._data_type = [
            subtype_type.__name__
            for subtype_type in self._types
            if hasattr(data_type, "__name__")
        ]

        self.data: Any | None = None

        self._complex_types: dict[
            AssertPath
            | AssertSet
            | Env
            | ImportInstance
            | ImportType
            | JsonData
            | JsonData
            | Pattern
            | RawFile,
            Callable[
                [str, type[Any]],
                AssertPath
                | AssertSet
                | Env
                | ImportInstance
                | ImportType
                | JsonData
                | JsonData
                | Pattern
                | RawFile,
            ],
        ] = {
            AssertPath: lambda _, __: AssertPath(),
            AssertSet: lambda name, subtype: AssertSet(name, subtype),
            Env: lambda envar, subtype: Env(envar, subtype),
            ImportInstance: lambda _, subtype: ImportInstance(subtype),
            ImportType: lambda _, subtype: ImportType(subtype),
            JsonFile: lambda _, subtype: JsonFile(subtype),
            JsonData: lambda _, subtype: JsonData(subtype),
            Paths: lambda _, subtype: Paths(subtype),
            Pattern: lambda _, subtype: Pattern(subtype),
            RawFile: lambda _, subtype: RawFile(subtype),
        }

        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in self._types

    @property
    def data_type(self):
        return ", ".join(self._data_type)

    async def parse(self, arg: str | None = None):
        result: Any | Exception | str = [arg]
        errs: list[Exception] = []

        for subtype in self._types:
            if complex_type_factory := self._complex_types.get(get_origin(subtype)):
                complex_type = complex_type_factory(self.name, subtype)

                errs = await asyncio.gather(
                    *[complex_type.parse(item) for item in result]
                )

                result = complex_type.data
                if not isinstance(result, list):
                    result = [result]

            errs = [err for err in errs if isinstance(err, Exception)]

            if len(errs) > 0:
                return errs[0]

        self.data = result

        return self
