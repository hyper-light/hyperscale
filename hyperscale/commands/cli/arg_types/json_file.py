from __future__ import annotations
import asyncio
import io
import json
from pydantic import BaseModel
from typing import Any, TypeVar, Generic
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar('T', bound=BaseModel)


class JsonFile(Generic[T]):

    def __init__(
        self,
        data_type: JsonFile[T]
    ):
        super().__init__()

        self.data: T | None = None

        conversion_type: T = reduce_pattern_type(data_type)

        self._data_type = [
            conversion_type.__name__
        ]

        self._types = tuple([
            conversion_type
        ])


        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in self._types

    @property
    def data_type(self):
        return ', '.join(self._data_type)
    
    async def parse(self, arg: str | None = None):
        try:

            if arg is None:
                return None
            
            file_handle: io.TextIOWrapper = await self._loop.run_in_executor(
                None,
                open,
                arg,
            )

            file_data = await self._loop.run_in_executor(
                None,
                file_handle.read
            )

            await self._loop.run_in_executor(
                None,
                file_handle.close
            )

            json_data: dict[str, Any] = json.loads(file_data)

            for conversion_type in self._types:
                if conversion_type == list:
                    self.data = json_data

                elif conversion_type == bytes:
                    self.data = bytes(json_data, encoding='utf-8')

                else:
                    self.data = conversion_type(**json_data)

                return self
            
        except Exception as e:
            return e