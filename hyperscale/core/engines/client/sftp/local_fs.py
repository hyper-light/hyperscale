import os
import posixpath
import sys
from typing import AsyncIterator
from .async_context_manager import async_context_manager
from .config import SFTPName
from .sftp_attrs import SFTPAttrs
from .constants import MAX_SFTP_READ_LEN, MAX_SFTP_WRITE_LEN
from .config import SFTPLimits
from .files import (
    setstat,
    to_local_path,
    from_local_path,
    make_sparse_file,
)
from .local_file import LocalFile
from .types import (
    SFTPPath,
)




class LocalFS:
    """An async wrapper around local filesystem access"""

    limits = SFTPLimits(0, MAX_SFTP_READ_LEN, MAX_SFTP_WRITE_LEN, 0)

    @staticmethod
    def basename(path: bytes) -> bytes:
        """Return the final component of a local file path"""

        return os.path.basename(path)

    def encode(self, path: SFTPPath) -> bytes:
        """Encode path name using filesystem native encoding

           This method has no effect if the path is already bytes.

        """

        # pylint: disable=no-self-use

        return os.fsencode(path)

    def compose_path(self, path: bytes,
                     parent: bytes | None = None) -> bytes:
        """Compose a path

           If parent is not specified, just encode the path.

        """

        path = self.encode(path)

        return posixpath.join(parent, path) if parent else path

    async def stat(self, path: bytes, *,
                   follow_symlinks: bool = True) -> 'SFTPAttrs':
        """Get attributes of a local file, directory, or symlink"""

        return SFTPAttrs.from_local(os.stat(to_local_path(path),
                                            follow_symlinks=follow_symlinks))

    async def setstat(self, path: bytes, attrs: 'SFTPAttrs', *,
                      follow_symlinks: bool = True) -> None:
        """Set attributes of a local file, directory, or symlink"""

        setstat(to_local_path(path), attrs, follow_symlinks=follow_symlinks)

    async def exists(self, path: bytes) -> bool:
        """Return if the local path exists and isn't a broken symbolic link"""

        return os.path.exists(to_local_path(path))

    async def isdir(self, path: bytes) -> bool:
        """Return if the local path refers to a directory"""

        return os.path.isdir(to_local_path(path))

    async def scandir(self, path: bytes) -> AsyncIterator[SFTPName]:
        """Return names and attributes of the files in a local directory"""

        with os.scandir(to_local_path(path)) as entries:
            for entry in entries:
                filename = entry.name

                if sys.platform == 'win32': # pragma: no cover
                    filename = os.fsencode(filename)

                attrs = SFTPAttrs.from_local(entry.stat(follow_symlinks=False))
                yield SFTPName(filename, attrs=attrs)

    async def mkdir(self, path: bytes) -> None:
        """Create a local directory with the specified attributes"""

        os.mkdir(to_local_path(path))

    async def readlink(self, path: bytes) -> bytes:
        """Return the target of a local symbolic link"""

        path = os.readlink(to_local_path(path))

        if sys.platform == 'win32' and \
                path.startswith('\\\\?\\'): # pragma: no cover
            path = path[4:]

        return from_local_path(path)

    async def symlink(self, oldpath: bytes, newpath: bytes) -> None:
        """Create a local symbolic link"""

        os.symlink(to_local_path(oldpath), to_local_path(newpath))

    @async_context_manager
    async def open(self, path: bytes, mode: str,
                   block_size: int = -1) -> LocalFile:
        """Open a local file"""

        # pylint: disable=unused-argument

        file_obj = open(to_local_path(path), mode)

        if mode[0] in 'wx':
            make_sparse_file(file_obj)

        return LocalFile(file_obj)