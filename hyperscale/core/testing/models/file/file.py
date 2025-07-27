import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar, Generic

from hyperscale.core.testing.models.base import OptimizedArg, FrozenDict
from .file_validator import FileValidator

T = TypeVar('T')


class File(OptimizedArg, Generic[T]):

    def __init__(
        self,
        path: str,
    ):
        super(
            File,
            self,
        ).__init__()

        validated_file = FileValidator(path=path)

        self.call_name: str | None = None
        self.data: File = FrozenDict(validated_file.model_dump(exclude_none=True))
        self.optimized: str | None = None

    async def optimize(
        self,
        path: str
    ):
        loop = asyncio.get_event_loop()

        with ThreadPoolExecutor() as executor:
            self.optimized = await loop.run_in_executor(
                executor,
                self._load_file,
                path,
            )
    
    def _load_file(
        self,
        path: str,
    ):
        with open(path, 'rb') as datafile:
            return datafile.read()
