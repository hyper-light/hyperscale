from __future__ import annotations
import asyncio
import io
from typing import Generic, TypeVar, Any
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar('T')


class RawFile(Generic[T]):

    def __init__(
        self,
        data_type: RawFile[T],
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
        return self._data_type
    

    async def parse(self, arg: str | None = None) -> T | Exception:

        if arg is None:
            return Exception('Err. - No match found.')
    
        result = await self._read_file(arg)
        if isinstance(result, Exception):
            return result
        
        self.data = result

        return self
    
    async def _read_file(self, arg: str | None = None):
                
        try:
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

            return self._parse_type(file_data)
            
        except Exception as e:
            return e
        
    def _parse_type(self, file_data: str):

        if self._type == bytes:
            return bytes(file_data, encoding='utf-8')

        return self._type(file_data)
    

        