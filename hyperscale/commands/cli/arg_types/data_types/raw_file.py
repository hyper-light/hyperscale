from __future__ import annotations
import asyncio
import io
from typing import Generic, TypeVar, Any
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar("T")


class RawFile(Generic[T]):
    def __init__(
        self,
        data_type: RawFile[T],
    ):
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
        return type(value) in self._types

    @property
    def data_type(self):
        return ", ".join(self._data_types)

    async def parse(self, arg: str | None = None) -> T | Exception:
        if arg is None:
            return Exception("Err. - No match found.")

        result = await self._read_file(arg)
        if isinstance(result, Exception):
            return result

        self.data = result

        return self

    async def _read_file(self, arg: str | None = None):
        try:
            file_data = await self._load_file(arg)
            if isinstance(file_data, Exception):
                return file_data

            return self._parse_type(file_data)

        except Exception as e:
            return Exception(
                f"encountered unexpected error {str(e)} loading file {arg}"
            )

    async def _load_file(self, arg: str):
        try:
            file_handle: io.TextIOWrapper = await self._loop.run_in_executor(
                None,
                open,
                arg,
            )

            file_data = await self._loop.run_in_executor(None, file_handle.read)

            await self._loop.run_in_executor(None, file_handle.close)

            return file_data

        except Exception as e:
            return Exception(f"encountered error {str(e)} opening file at {arg}")

    def _parse_type(self, file_data: str):
        for conversion_type in self._types:
            try:
                if conversion_type == bytes:
                    return bytes(file_data, encoding="utf-8")

                return conversion_type(file_data)

            except Exception:
                pass

        return Exception(f"could not parse {file_data} specified types")
