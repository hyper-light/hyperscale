import asyncio
import functools
import pathlib
from concurrent.futures import ThreadPoolExecutor
from pydantic import BaseModel, StrictStr
from hyperscale.core.engines.client.ssh.protocol.ssh.constants import (
    FILEXFER_TYPE_SYMLINK,
    FILEXFER_TYPE_REGULAR,
    FILEXFER_TYPE_DIRECTORY,
)

from .file_attributes import FileAttributes
from .transfer_result import TransferResult


class FileGlob(BaseModel):
    pattern: StrictStr

    async def load(
        self,
        loop: asyncio.AbstractEventLoop,
        path: StrictStr | pathlib.Path,
        attributes: FileAttributes,
        encoding: str,
    ):
        with ThreadPoolExecutor() as exc:
            paths = await loop.run_in_executor(
                exc,
                self._find_files,
                path,
            )

            return await asyncio.gather(*[
                loop.run_in_executor(
                    exc,
                    functools.partial(
                        self._load_file,
                        path,
                        attributes,
                        encoding,
                    )
                ) for path in paths
            ])

    def _find_files(
        self,
        path: str | pathlib.Path,
    ):
        if isinstance(path, pathlib.Path):
            return path.rglob(self.pattern)
        
        return pathlib.Path(path).rglob(self.pattern)
    
    def _load_file(
        self,
        path: pathlib.Path,
        attributes: FileAttributes,
        encoding: str,
    ) -> tuple[bytes, FileAttributes, bytes | None]:
        path_str = str(path)

        if path.is_symlink():
            destination = str(path.resolve()).encode(encoding=encoding)
            dstpath: bytes = str(path).encode(encoding=encoding)

            attributes = attributes.model_copy(update={
                "type": FILEXFER_TYPE_SYMLINK
            })
            
            return (
                dstpath,
                destination,
                attributes,
            )
        
        elif path.is_dir():
            attributes = attributes.model_copy(update={
                "type": FILEXFER_TYPE_DIRECTORY
            })

            dstpath: bytes = str(path).encode(encoding=encoding)

            return (
                dstpath,
                None,
                attributes,
            )
            

        attributes = attributes.model_copy(update={
            "type": FILEXFER_TYPE_REGULAR
        })
        
        dstpath: bytes = str(path).encode(encoding=encoding)
        with open(path_str, 'rb') as data_file:
            return (
                dstpath,
                data_file.read(),
                attributes,
            )