from __future__ import annotations
import asyncio
import io
from typing import Generic, TypeVar, Any
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar('T')


class RawFile(Generic[T]):

    def __init__(
        self,
        data_type: RawFile[tuple[T]],
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
    

    async def parse(self, arg: str | None = None) -> T | Exception:
        try:

            if arg is None:
                return Exception('Err. - No match found.')
            
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

            for conversion_type in self._types:
                if conversion_type == bytes:
                    self.data = bytes(file_data, encoding='utf-8')

                    return self
                
                else:
                    self.data = conversion_type(file_data)

                    return self
            
        except Exception as e:
            return e

        