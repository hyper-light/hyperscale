from __future__ import annotations
import asyncio
import io
import json
from pydantic import BaseModel
from typing import Any, TypeVar, Generic
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar("T", bound=BaseModel)


class JsonData(Generic[T]):
    def __init__(self, data_type: JsonData[T]):
        super().__init__()

        self.data: T | None = None

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

    async def parse(self, arg: str | list[str] | None = None):
        result = await self._load_json(arg)
        if isinstance(result, Exception):
            return result

        self.data = result

        return self

    async def _load_json(self, arg: str):
        json_data: dict[str, Any] = json.loads(arg)

        for conversion_type in self._types:
            try:
                if conversion_type == list and isinstance(json_data, list):
                    return json_data

                elif conversion_type == bytes:
                    return bytes(json_data, encoding="utf-8")

                return conversion_type(**json_data)

            except Exception:
                pass

        return Exception(f"could not parse {arg} specified types")
