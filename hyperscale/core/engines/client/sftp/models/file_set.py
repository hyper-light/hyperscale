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


class FileSet(BaseModel):
    attributes: FileAttributes
    path: StrictStr | pathlib.Path
    pattern: StrictStr

    async def load(
        self,
        loop: asyncio.AbstractEventLoop,
        encoding: str,
    ):
        with ThreadPoolExecutor() as exc:
            paths = await loop.run_in_executor(
                exc,
                self._find_files,
            )

            return await asyncio.gather(*[
                loop.run_in_executor(
                    exc,
                    functools.partial(
                        self._load_file,
                        path,
                        encoding,
                    )
                ) for path in paths
            ])

    def _find_files(self):
        if isinstance(self.path, pathlib.Path):
            return self.path.rglob(self.pattern)
        
        return pathlib.Path(self.path).rglob(self.pattern)
    
    def _load_file(
        self,
        path: pathlib.Path,
        encoding: str,
    ) -> tuple[bytes, FileAttributes, bytes | None]:
        path_str = str(path)

        if path.is_symlink():
            destination = str(path.resolve()).encode(encoding=encoding)
            dstpath: bytes = str(path).encode(encoding=encoding)

            attributes = self.attributes.model_copy(update={
                "type": FILEXFER_TYPE_SYMLINK
            })
            
            return (
                dstpath,
                destination,
                attributes,
            )
        
        elif path.is_dir():
            attributes = self.attributes.model_copy(update={
                "type": FILEXFER_TYPE_DIRECTORY
            })

            dstpath: bytes = str(path).encode(encoding=encoding)

            return (
                dstpath,
                None,
                attributes,
            )
            

        attributes = self.attributes.model_copy(update={
            "type": FILEXFER_TYPE_REGULAR
        })
        
        dstpath: bytes = str(path).encode(encoding=encoding)
        with open(path_str, 'rb') as data_file:
            return (
                dstpath,
                data_file.read(),
                attributes,
            )