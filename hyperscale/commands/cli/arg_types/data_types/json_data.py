from __future__ import annotations
import asyncio
import io
import json
from pydantic import BaseModel
from typing import Any, TypeVar, Generic
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar('T', bound=BaseModel)


class JsonData(Generic[T]):

    def __init__(
        self,
        data_type: JsonData[T]
    ):
        super().__init__()

        self.data: T | None = None

        conversion_type: T = reduce_pattern_type(data_type)

        self._data_type = conversion_type.__name__ if hasattr(conversion_type, '__name__') else type(conversion_type).__name__
        self._type = conversion_type

        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in [self._type]

    @property
    def data_type(self):
        return ', '.join(self._data_type)
    
    async def parse(self, arg: str | list[str] | None = None):
        
        result = await self._load_json(arg)
        if isinstance(result, Exception):
            return result
        
        self.data = result

        return self

    
    async def _load_json(self, arg: str):
        try:

            json_data: dict[str, Any] = json.loads(arg)

            if self._type == list and isinstance(json_data, list):
                return json_data

            elif self._type == bytes:
                return bytes(json_data, encoding='utf-8')

            return self._type(**json_data)
            
        except Exception as e:
            return e