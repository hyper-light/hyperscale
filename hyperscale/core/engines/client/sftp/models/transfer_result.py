from __future__ import annotations
import msgspec
from typing import Literal

from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs, SFTPVFSAttrs
from hyperscale.core.testing.models.file.file_attributes import FileAttributes
from .filesystem_attributes import FilesystemAttributes


ResultFileType = Literal[
    "FILE",
    "DIRECTORY",
    "SYMLINK",
    "SPECIAL",
    "UNKNOWN",
    "SOCKET",
    "CHAR_DEVICE",
    "BLOCK_DEVICE", 
    "FIFO",
    "PACKET",
    "DATA",
    "OTHER",
    "STATS",
    "HARDLINK",
]

class TransferResult(msgspec.Struct):
    file_path: bytes
    file_type: ResultFileType | None = "FILE"
    file_listing: list[TransferResult] | None = None
    file_data: bytes | None = None
    file_transfer_elapsed: int | float = 0
    file_attribues: FileAttributes | None = None
    filesystem_attributes: FilesystemAttributes | None = None
    file_transfer_at_end: bool = True

    @classmethod
    def to_attributes(
        cls,
        attrs: SFTPAttrs,
    ):
        return FileAttributes(
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
    
    @classmethod
    def to_filesystem_attributes(
        cls,
        attrs: SFTPVFSAttrs
    ):
        return FilesystemAttributes(
            bavail=attrs.bavail,
            bfree=attrs.bfree,
            blocks=attrs.blocks,
            bsize=attrs.bsize,
            favail=attrs.favail,
            ffree=attrs.ffree,
            files=attrs.files,
            flags=attrs.flags,
            frsize=attrs.frsize,
            fsid=attrs.fsid,
            namemax=attrs.namemax,
        )
    
    @classmethod
    def to_file_type(
        cls,
        file_type: int,
    ) -> ResultFileType:
        
        match file_type:
            case 1:
                return "FILE"
            
            case 2:
                return "DIRECTORY"
            
            case 3:
                return "SYMLINK"

            case 4:
                return "SPECIAL"
            
            case 5:
                return "UNKNOWN"
            
            case 6:
                return "SOCKET"
            
            case 7:
                return "CHAR_DEVICE"
            
            case 8:
                return "BLOCK_DEVICE"
            
            case 9:
                return "FIFO"
            
            case _:
                return "OTHER"
            
    @classmethod
    def to_file_type_int(
        cls,
        file_type: ResultFileType,
    ):
        match file_type:
            case "FILE":
                return 1
            
            case "DIRECTORY":
                return 2
            
            case "SYMLINK":
                return 3
            
            case "SPECIAL":
                return 4
            
            case "UNKNOWN":
                return 5
            
            case "SOCKET":
                return 6
            
            case "CHAR_DEVICE":
                return 7
            
            case "BLOCK_DEVICE":
                return 8
            
            case "FIFO":
                return 9
            
            case _:
                return 1
