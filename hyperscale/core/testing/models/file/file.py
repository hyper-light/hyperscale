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
from .file_validator import FileValidator

T = TypeVar('T')


class File(OptimizedArg, Generic[T]):

    def __init__(
        self,
        path: str | pathlib.Path | tuple[str | pathlib.Path, str],
    ):
        super(
            File,
            self,
        ).__init__()
        
        path_encoding: str | None = None
        if isinstance(path, tuple):
            filepath, path_encoding = path

        else:
            filepath = path

        validated_file = FileValidator(
            path=filepath,
            path_encoding=path_encoding,
        )

        self.call_name: str | None = None
        self.data: dict[str, str] = FrozenDict(validated_file.model_dump(exclude_none=True))
        self.optimized: tuple[bytes, bytes | None, FileAttributes] = None
        self.encoding: str = "application/octet-stream"

    async def optimize(
        self,
        request_type: RequestType,
    ):
        
        if self.optimized is not None:
            return

        match request_type:
            case RequestType.HTTP | RequestType.SCP | RequestType.SFTP:
                loop = asyncio.get_event_loop()

                path_encoding = self.data.get("path_encoding")

                with ThreadPoolExecutor() as executor:
                    self.optimized = await loop.run_in_executor(
                        executor,
                        self._load_file,
                        path_encoding,
                    )

            case _:
                pass
            
    def _load_file(
        self,
        path_encoding: str,
    ):
        
        filepath = self.data("path")
        if isinstance(filepath, str):
            filepath = pathlib.Path(filepath)

        attributes = FileAttributes.from_stat(filepath)
        self.encoding = attributes.encoding if attributes.encoding else self.encoding

        if filepath.is_symlink():
            filepath = filepath.resolve()
            attributes = FileAttributes.from_stat(filepath)

        if filepath.is_dir():
            attributes.type = FILEXFER_TYPE_DIRECTORY
            dstpath = str(filepath).encode(encoding=path_encoding)

            return (
                dstpath,
                None,
                attributes,
            )

        else:  
            attributes.type = FILEXFER_TYPE_REGULAR
        
        dstpath = str(filepath)
        with open(dstpath, 'rb') as data_file:
            return  (
                dstpath.encode(encoding=path_encoding),
                data_file.read(),
                attributes,
            )