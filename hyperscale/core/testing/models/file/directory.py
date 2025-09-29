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

from .directory_validator import DirectoryValidator
from .file_attributes import FileAttributes


T = TypeVar('T')


class Directory(OptimizedArg, Generic[T]):

    def __init__(
        self,
        path: str | pathlib.Path | tuple[str | pathlib.Path, str],
    ):
        super(
            Directory,
            self,
        ).__init__()

        path_encoding: str | None = None
        if isinstance(path, tuple):
            filepath, path_encoding = path

        else:
            filepath = path

        validated_directory = DirectoryValidator(
            path=filepath,
            path_encoding=path_encoding,
        )

        self.call_name: str | None = None
        self.data: dict[str, str | pathlib.Path] = FrozenDict(validated_directory.model_dump(exclude_none=True))
        self.optimized: list[tuple[bytes, bytes | None, FileAttributes]] = None
        self._path = filepath

    async def optimize(
        self,
        request_type: RequestType,
    ):
        if self.optimized is not None:
            return

        match request_type:
            case RequestType.SCP | RequestType.SFTP:
                loop = asyncio.get_event_loop()

                with ThreadPoolExecutor() as exc:

                    files = await loop.run_in_executor(
                        exc,
                        self._find_files,
                    )

                    path_encoding = self.data.get("path_encoding")
                    
                    self.optimized =  await asyncio.gather(*[
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

        path = self.data.get("path")
        if isinstance(path, str):
            path = pathlib.Path(path)

        return [
            (
                item,
                FileAttributes.from_stat(item)
            ) for item in path.iterdir() if item.is_file()
        ]
    
    def _load_file(
        self,
        filepath: pathlib.Path,
        attributes: FileAttributes,
        path_encoding: str,
    ):
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
            return (
                dstpath.encode(encoding=path_encoding),
                data_file.read(),
                attributes,
            )