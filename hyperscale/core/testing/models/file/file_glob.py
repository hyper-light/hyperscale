import asyncio
import pathlib
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar, Generic

from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.testing.models.base import OptimizedArg, FrozenDict

from .constants import (
    FILEXFER_TYPE_REGULAR,
    FILEXFER_TYPE_DIRECTORY,
)
from .file_attributes import FileAttributes
from .file_glob_validator import FileGlobValidator


T = TypeVar('T')


class FileGlob(OptimizedArg, Generic[T]):

    def __init__(
        self,
        files: (
            str
            | tuple[str | pathlib.Path, str]
            | tuple[str | pathlib.Path, str]
        ),
    ):
        super(
            FileGlob,
            self,
        ).__init__()

        if not isinstance(files, (tuple, str)):
            FileGlobValidator(
                filepath=files,
            )

        path_encoding: str | None = None
        if isinstance(files, tuple) and len(files) > 2:
            filepath, pattern, path_encoding = files

        elif isinstance(files, tuple) and len(files) > 1:
            filepath, pattern = files

        elif isinstance(files, tuple):
            filepath = files[0]
            pattern = "*"

        else:
            filepath = files
            pattern = "*"

        validated_files = FileGlobValidator(
            filepath=filepath,
            pattern=pattern,
            path_encoding=path_encoding,
        )

        self.call_name: str | None = None
        self.data: dict[str, str | pathlib.Path | None] = FrozenDict(validated_files.model_dump(exclude_none=True))
        self.optimized: list[tuple[bytes, bytes | None, FileAttributes]] = None
        self._files = files

    async def optimize(
        self,
        request_type: RequestType,
    ):
        
        if self.optimized is not None:
            return
        
        match request_type:
            case RequestType.SFTP:

                loop = asyncio.get_event_loop()
                path_encoding = self.data.get("path_encoding")
                
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
                            path_encoding,
                        ) for file, attributes in files
                    ])

            case _:
                pass
            
    def _find_files(self):

        filepath: str | pathlib.Path = self.data.get("filepath")
        pattern: str = self.data.get("pattern")

        if isinstance(filepath, str):
            filepath = pathlib.Path(filepath)
        
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
            path = path.resolve()
            attributes = FileAttributes.from_stat(path)

        if path.is_dir():

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