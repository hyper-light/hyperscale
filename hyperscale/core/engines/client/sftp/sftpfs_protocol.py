
from typing import Protocol, AsyncIterator
from .async_context_manager import async_context_manager
from .config import SFTPLimits, SFTPName
from .sftp_attrs import SFTPAttrs
from .sftp_file_protocol import SFTPFileProtocol
from .types import SFTPPath



class SFTPFSProtocol(Protocol):
    """Protocol for accessing a filesystem via an SFTP server"""

    @property
    def limits(self) -> SFTPLimits:
        """SFTP server limits associated with this SFTP session"""

    @staticmethod
    def basename(path: bytes) -> bytes:
        """Return the final component of a POSIX-style path"""

    def encode(self, path: SFTPPath) -> bytes:
        """Encode path name using configured path encoding"""

    def compose_path(self, path: bytes,
                     parent: bytes | None = None) -> bytes:
        """Compose a path"""

    async def stat(self, path: bytes, *,
                   follow_symlinks: bool = True) -> SFTPAttrs:
        """Get attributes of a file, directory, or symlink"""

    async def setstat(self, path: bytes, attrs: SFTPAttrs, *,
                      follow_symlinks: bool = True) -> None:
        """Set attributes of a file, directory, or symlink"""

    async def isdir(self, path: bytes) -> bool:
        """Return if the path refers to a directory"""

    def scandir(self, path: bytes) -> AsyncIterator[SFTPName]:
        """Return names and attributes of the files in a directory"""

    async def mkdir(self, path: bytes) -> None:
        """Create a directory"""

    async def readlink(self, path: bytes) -> bytes:
        """Return the target of a symbolic link"""

    async def symlink(self, oldpath: bytes, newpath: bytes) -> None:
        """Create a symbolic link"""

    @async_context_manager
    async def open(self, path: bytes, mode: str,
                   block_size: int = -1) -> SFTPFileProtocol:
        """Open a file"""

