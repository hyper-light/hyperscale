import asyncio
import pathlib
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar, Generic
from hyperscale.core.testing.models.base import OptimizedArg, FrozenDict
from .constants import (
    FILEXFER_TYPE_SYMLINK,
    FILEXFER_TYPE_REGULAR,
    FILEXFER_TYPE_DIRECTORY,
)

from .file_attributes import FileAttributes
from .file_validator import FileValidator

T = TypeVar('T')


class File(OptimizedArg, Generic[T]):

    def __init__(
        self,
        path: str | pathlib.Path,
    ):
        super(
            File,
            self,
        ).__init__()

        validated_file = FileValidator(path=path)

        self.call_name: str | None = None
        self.data: dict[str, str] = FrozenDict(validated_file.model_dump(exclude_none=True))
        self.optimized: tuple[bytes, bytes | None, FileAttributes] = None
        self._path = path

    async def optimize(
        self,
        encoding: str = 'utf-8',
    ):
        loop = asyncio.get_event_loop()

        with ThreadPoolExecutor() as executor:
            self.optimized = await loop.run_in_executor(
                executor,
                self._load_file,
                encoding,
            )
    
   
    def _load_file(
        self,
        encoding: str,
    ):
        
        if isinstance(self._path, str):
            path = pathlib.Path(self._path)

        else:
            path = self._path
        
        attributes = FileAttributes.from_stat(path)
        
        if isinstance(self._path, str):
            path = pathlib.Path(self._path)
            path_str = self._path

        else:
            path = self._path
            path_str = str(self._path)

        if path.is_symlink():
            destination = str(path.resolve()).encode(encoding=encoding)
            dstpath: bytes = str(path).encode(encoding=encoding)

            attributes.type = FILEXFER_TYPE_SYMLINK
            
            return (
                dstpath,
                destination,
                attributes,
            )
        
        elif path.is_dir():

            attributes.type = FILEXFER_TYPE_DIRECTORY
            dstpath: bytes = str(path).encode(encoding=encoding)

            return (
                dstpath,
                None,
                attributes,
            )

        else:  
            attributes.type = FILEXFER_TYPE_REGULAR
        
        dstpath: bytes = str(path).encode(encoding=encoding)
        with open(path_str, 'rb') as data_file:
            return (
                dstpath,
                data_file.read(),
                attributes,
            )