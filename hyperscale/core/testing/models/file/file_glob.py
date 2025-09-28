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
from .file_glob_validator import FileGlobValidator


T = TypeVar('T')


class FileGlob(OptimizedArg, Generic[T]):

    def __init__(
        self,
        files: tuple[str | pathlib.Path, str],
    ):
        super(
            FileGlob,
            self,
        ).__init__()

        validated_files = FileGlobValidator(files=files)

        self.call_name: str | None = None
        self.data: dict[str, tuple[str, str]] = FrozenDict(validated_files.model_dump(exclude_none=True))
        self.optimized: list[tuple[bytes, bytes | None, FileAttributes]] = None
        self._files = files

    async def optimize(
        self,
        encoding: str = 'utf-8',
    ):
        loop = asyncio.get_event_loop()

        with ThreadPoolExecutor() as exc:

            files = await loop.run_in_executor(
                exc,
                self._find_files,
            )

            return await asyncio.gather(*[
                loop.run_in_executor(
                    exc,
                    self._load_file,
                    file,
                    attributes,
                    encoding,
                ) for file, attributes in files
            ])

    def _find_files(self):

        filepath, pattern = self._files

        if isinstance(filepath, pathlib.Path):
            return [
                (
                    path,
                    FileAttributes.from_stat(path)
                ) for path in filepath.rglob(pattern)
            ]
        
        
        return [
            (
                path,
                FileAttributes.from_stat(path)
            )
            for path in pathlib.Path(filepath).rglob(pattern)
        ]
    
    def _load_file(
        self,
        path: pathlib.Path,
        attributes: FileAttributes,
        encoding: str,
    ):
        path_str = str(path)

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