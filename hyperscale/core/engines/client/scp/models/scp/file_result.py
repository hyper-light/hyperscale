
from typing import Literal

from pydantic import BaseModel, StrictBytes, StrictInt, StrictFloat

from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs
from .file_attributes import FileAttributes


class FileResult(BaseModel):
    path: StrictBytes
    type: Literal["FILE", "DIRECTORY"] = "FILE"
    data: StrictBytes | bytearray | None = None
    result_elapsed: StrictInt | StrictFloat = 0
    attribues: FileAttributes | None = None


    def set_attributes(
        self,
        attrs: SFTPAttrs,
    ):
        return self.model_copy(update={
            "attributes": FileAttributes(
                size=attrs.size,
                alloc_size=attrs.alloc_size,
                atime=attrs.atime,
                atime_ns=attrs.atime_ns,
                crtime=attrs.crtime,
                crtime_ns=attrs.crtime_ns,
                mime_type=attrs.mime_type,
                mtime=attrs.mtime,
                mtime_ns=attrs.mtime_ns,
                ctime=attrs.ctime,
                ctime_ns=attrs.ctime_ns,
                nlink=attrs.nlink,
                
            )
        })
        