import os
import stat
import sys
from pathlib import Path
from  typing import Literal, overload
from .constants import (
    FXF_READ,
    FXF_WRITE,
    FXF_CREAT,
    FXF_TRUNC,
    FXF_APPEND,
    FXF_EXCL,
)
from .error import SFTPOwnerInvalid, SFTPGroupInvalid
from .sftp_attrs import SFTPAttrs
from .types import SFTPPath, LocalPath, IO, FilePath
from .units import tuple_to_nsec



open_modes = {
    'r':  FXF_READ,
    'w':  FXF_WRITE | FXF_CREAT | FXF_TRUNC,
    'a':  FXF_WRITE | FXF_CREAT | FXF_APPEND,
    'x':  FXF_WRITE | FXF_CREAT | FXF_EXCL,

    'r+': FXF_READ | FXF_WRITE,
    'w+': FXF_READ | FXF_WRITE | FXF_CREAT | FXF_TRUNC,
    'a+': FXF_READ | FXF_WRITE | FXF_CREAT | FXF_APPEND,
    'x+': FXF_READ | FXF_WRITE | FXF_CREAT | FXF_EXCL
}


def lookup_uid(user: str | None) -> int | None:
    """Return the uid associated with a user name"""

    if user is not None:
        try:
            # pylint: disable=import-outside-toplevel
            import pwd
            uid = pwd.getpwnam(user).pw_uid
        except (ImportError, KeyError):
            try:
                uid = int(user)
            except ValueError:
                raise SFTPOwnerInvalid(f'Invalid owner: {user}') from None
    else:
        uid = None

    return uid


def lookup_gid(group: str | None) -> int | None:
    """Return the gid associated with a group name"""

    if group is not None:
        try:
            # pylint: disable=import-outside-toplevel
            import grp
            gid = grp.getgrnam(group).gr_gid
        except (ImportError, KeyError):
            try:
                gid = int(group)
            except ValueError:
                raise SFTPGroupInvalid(f'Invalid group: {group}') from None
    else:
        gid = None

    return gid


def mode_to_pflags(mode: str) -> tuple[int, bool]:
    """Convert open mode to SFTP open flags"""

    if 'b' in mode:
        mode = mode.replace('b', '')
        binary = True
    else:
        binary = False

    pflags = open_modes.get(mode)

    if not pflags:
        raise ValueError(f'Invalid mode: {mode!r}')

    return pflags, binary



def from_local_path(path: SFTPPath) -> bytes:
    """Convert local path to SFTP path"""

    path = os.fsencode(path)

    if sys.platform == 'win32': # pragma: no cover
        path = path.replace(b'\\', b'/')

        if path[:1] != b'/' and path[1:2] == b':':
            path = b'/' + path

    return path


def to_local_path(path: bytes) -> LocalPath:
    """Convert SFTP path to local path"""

    if sys.platform == 'win32': # pragma: no cover
        path = os.fsdecode(path)

        if path[:1] == '/' and path[2:3] == ':':
            path = path[1:]

        path = path.replace('/', '\\')
    else:
        path = os.fsencode(path)

    return path


def setstat(
    path: int | SFTPPath,
    attrs: 'SFTPAttrs', *,
    follow_symlinks: bool = True,
) -> None:
    """Utility function to set file attributes"""

    if attrs.size is not None:
        os.truncate(path, attrs.size)

    uid = lookup_uid(attrs.owner) if attrs.uid is None else attrs.uid
    gid = lookup_gid(attrs.group) if attrs.gid is None else attrs.gid

    atime_ns = tuple_to_nsec(attrs.atime, attrs.atime_ns) \
        if attrs.atime is not None else None

    mtime_ns = tuple_to_nsec(attrs.mtime, attrs.mtime_ns) \
        if attrs.mtime is not None else None

    if ((atime_ns is None and mtime_ns is not None) or
            (atime_ns is not None and mtime_ns is None)):
        stat_result = os.stat(path, follow_symlinks=follow_symlinks)

        if atime_ns is None and mtime_ns is not None:
            atime_ns = stat_result.st_atime_ns

        if atime_ns is not None and mtime_ns is None:
            mtime_ns = stat_result.st_mtime_ns

    if uid is not None and gid is not None:
        try:
            os.chown(path, uid, gid, follow_symlinks=follow_symlinks)
        except NotImplementedError: # pragma: no cover
            pass
        except AttributeError: # pragma: no cover
            raise NotImplementedError from None

    if attrs.permissions is not None:
        try:
            os.chmod(path, stat.S_IMODE(attrs.permissions),
                     follow_symlinks=follow_symlinks)
        except NotImplementedError: # pragma: no cover
            pass

    if atime_ns is not None and mtime_ns is not None:
        try:
            os.utime(path, ns=(atime_ns, mtime_ns),
                     follow_symlinks=follow_symlinks)
        except NotImplementedError: # pragma: no cover
            pass

if sys.platform == 'win32' and _pywin32_available: # pragma: no cover
    def make_sparse_file(file_obj: IO) -> None:
        """Enable sparse file support on a file on Windows"""

        handle = msvcrt.get_osfhandle(file_obj.fileno())

        win32file.DeviceIoControl(handle, winioctlcon.FSCTL_SET_SPARSE,
                                  b'', 0, None)
else:
    def make_sparse_file(_file_obj: IO) -> None:
        """Sparse files are automatically enabled on non-Windows systems"""



def open_file(filename: FilePath, mode: str, buffering: int = -1) -> IO[bytes]:
    """Open a file with home directory expansion"""

    return open(Path(filename).expanduser(), mode, buffering=buffering)


@overload
def read_file(filename: FilePath) -> bytes:
    """Read from a binary file with home directory expansion"""

@overload
def read_file(filename: FilePath, mode: Literal['rb']) -> bytes:
    """Read from a binary file with home directory expansion"""

@overload
def read_file(filename: FilePath, mode: Literal['r']) -> str:
    """Read from a text file with home directory expansion"""

def read_file(filename, mode = 'rb'):
    """Read from a file with home directory expansion"""

    with open_file(filename, mode) as f:
        return f.read()


def write_file(filename: FilePath, data: bytes, mode: str = 'wb') -> int:
    """Write or append to a file with home directory expansion"""

    with open_file(filename, mode) as f:
        return f.write(data)
