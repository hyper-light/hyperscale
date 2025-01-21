from __future__ import annotations

from pydantic import BaseModel
from typing import TypeVar, Generic, get_args, Any, get_origin
from .chain import Chain
from .map import Map

T = TypeVar("T")
K = TypeVar("K")


class Operator(Generic[T, K]):
    def __init__(self, name: str, data_types: Operator[T]):
        super().__init__()

        self.name = name
        data_types, conversion_type = get_args(data_types)

        self._types = data_types
        self._conversion_type = conversion_type

        self._data_type = [data_type.__name__ for data_type in self._types]

        self.data: K = None

    def __contains__(self, value: Any):
        return type(value) in self._types

    @property
    def data_type(self):
        return ", ".join(self._data_type)

    async def parse(self, arg: str | None = None):
        result: Any | None = None

        if get_origin(self._types) == Chain:
            chain = Chain(self.name, self._types)

            err = await chain.parse(arg)
            if isinstance(err, Exception):
                return err

            result = chain.data

        elif get_origin(self._types) == Map:
            map_operator = Map(self.name, self._types)

            err = await map_operator.parse(arg)
            if isinstance(err, Exception):
                return err

            result = map_operator.data

        try:
            if BaseModel in self._conversion_type.__bases__:
                return self._conversion_type(**result)

            self.data = self._conversion_type(result)

        except Exception as e:
            return e

        return self
