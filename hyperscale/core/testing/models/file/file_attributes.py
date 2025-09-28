import msgspec
import mimetypes
import pathlib


class FileAttributes(msgspec.Struct):
    type: int | None = None
    size: int | None = None
    alloc_size: int | None = None
    uid: int | None = None
    gid: int | None = None
    owner: str | None = None
    group: str | None = None
    permissions: int | None = None
    atime: int | float | None = None
    atime_ns: int | float | None = None
    crtime: int | float | None = None
    crtime_ns: int | float | None = None
    mtime: int | float | None = None
    mtime_ns: int | float | None = None
    ctime: int | float | None = None
    ctime_ns: int | float | None = None
    mime_type: str | None = None
    nlink: int | None = None

    
    @classmethod
    def from_stat(
        self,
        path: pathlib.Path
    ):
        
        result = path.stat()
        mime_type, _ = mimetypes.guess_file_type(path)

        path_type = 1

        if path.is_dir():
            path_type = 2

        elif path.is_symlink():
            path_type = 3


        elif path.is_socket():
            path_type = 6

        elif path.is_char_device():
            path_type = 7

        elif path.is_block_device():
            path_type = 8

        elif path.is_fifo():
            path_type = 9

        return FileAttributes(
            type=path_type,
            uid=result.st_uid,
            gid=result.st_gid,
            permissions=result.st_mode,
            atime=result.st_atime,
            atime_ns=result.st_atime_ns,
            ctime=result.st_ctime,
            crtime=result.st_ctime,
            crtime_ns=result.st_ctime_ns,
            mtime=result.st_mtime,
            mtime_ns=result.st_mtime_ns,
            mime_type=mime_type,
            nlink=result.st_nlink,
        )
        