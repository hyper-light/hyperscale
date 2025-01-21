from __future__ import annotations
import asyncio
from typing import TypeVar, Generic, Any
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar("T")


class AssertSet(Generic[T]):
    def __init__(self, name: str, data_type: AssertSet[T]):
        super().__init__()
        self.name = name
        self.data: T | None = None

        conversion_types: list[T] = reduce_pattern_type(data_type)

        self._data_types = [
            conversion_type.__name__
            if hasattr(conversion_type, "__name__")
            else conversion_type
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
        if arg is None:
            return Exception("no argument passed")

        try:
            assert arg in self._types, (
                f"{arg} is not a supported value for {self.name} - please pass one of {self.data_type}"
            )

            self.data = arg

            return self

        except Exception as e:
            return e
