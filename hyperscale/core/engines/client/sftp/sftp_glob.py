import posixpath
from fnmatch import fnmatch
from typing import Sequence, AsyncIterator, Protocol
from .constants import (
    FILEXFER_TYPE_DIRECTORY,
    MIN_SFTP_VERSION,
)
from .sftp_attrs import SFTPAttrs
from .config import SFTPName
from .error import (
    SFTPNoSuchFile,
    SFTPPermissionDenied,
    SFTPNoSuchPath,
    SFTPError,
)
from .types import SFTPErrorHandler, SFTPPathList



class SFTPGlobProtocol(Protocol):
    """Protocol for getting files to perform glob matching against"""

    async def stat(self, path: bytes) -> 'SFTPAttrs':
        """Get attributes of a file"""

    def scandir(self, path: bytes) -> AsyncIterator['SFTPName']:
        """Return names and attributes of the files in a directory"""


class SFTPGlob:
    """SFTP glob matcher"""

    def __init__(
        self,
        fs: SFTPGlobProtocol,
        multiple=False,
    ):
        self._fs = fs
        self._multiple = multiple
        self._prev_matches: set[bytes] = set()
        self._new_matches: list[SFTPName] = []
        self._matched = False
        self._stat_cache: dict[bytes, SFTPAttrs | None] = {}
        self._scandir_cache: dict[bytes, list[SFTPName]] = {}

    def _split(self, pattern: bytes) -> tuple[bytes, SFTPPathList]:
        """Split out exact parts of a glob pattern"""

        patlist: SFTPPathList = []

        if any(c in pattern for c in b'*?[]'):
            path = b''
            plain: list[bytes] = []

            for current in pattern.split(b'/'):
                if any(c in current for c in b'*?[]'):
                    if plain:
                        if patlist:
                            patlist.append(plain)
                        else:
                            path = b'/'.join(plain) or b'/'

                        plain = []

                    patlist.append(current)
                else:
                    plain.append(current)

            if plain:
                patlist.append(plain)
        else:
            path = pattern

        return path, patlist

    def _report_match(self, path, attrs):
        """Report a matching name"""

        self._matched = True

        if self._multiple:
            if path not in self._prev_matches:
                self._prev_matches.add(path)
            else:
                return

        self._new_matches.append(SFTPName(path, attrs=attrs))

    async def _stat(self, path) -> SFTPAttrs | None:
        """Cache results of calls to stat"""

        try:
            return self._stat_cache[path]
        except KeyError:
            pass

        try:
            attrs = await self._fs.stat(path)
        except (SFTPNoSuchFile, SFTPPermissionDenied, SFTPNoSuchPath):
            attrs = None

        self._stat_cache[path] = attrs
        return attrs

    async def _scandir(self, path) -> AsyncIterator[SFTPName]:
        """Cache results of calls to scandir"""

        try:
            for entry in self._scandir_cache[path]:
                yield entry

            return
        except KeyError:
            pass

        entries: list[SFTPName] = []

        try:
            async for entry in self._fs.scandir(path):
                entries.append(entry)
                yield entry
        except (SFTPNoSuchFile, SFTPPermissionDenied, SFTPNoSuchPath):
            pass

        self._scandir_cache[path] = entries

    async def _match_exact(self, path: bytes, pattern: Sequence[bytes],
                           patlist: SFTPPathList) -> None:
        """Match on an exact portion of a path"""

        newpath = posixpath.join(path, *pattern)
        newpatlist = patlist[1:]

        attrs = await self._stat(newpath)

        if attrs is None:
            return

        if newpatlist:
            if attrs.type == FILEXFER_TYPE_DIRECTORY:
                await self._match(newpath, attrs, newpatlist)
        else:
            self._report_match(newpath, attrs)

    async def _match_pattern(self, path: bytes, attrs: SFTPAttrs,
                             pattern: bytes, patlist: SFTPPathList) -> None:
        """Match on a pattern portion of a path"""

        newpatlist = patlist[1:]

        if pattern == b'**':
            if newpatlist:
                await self._match(path, attrs, newpatlist)
            else:
                self._report_match(path, attrs)

        async for entry in self._scandir(path or b'.'):
            filename = entry.filename

            if filename in (b'.', b'..'):
                continue

            if not pattern or fnmatch(filename, pattern):
                newpath = posixpath.join(path, filename)
                attrs = entry.attrs

                if pattern == b'**' and attrs.type == FILEXFER_TYPE_DIRECTORY:
                    await self._match(newpath, attrs, patlist)
                elif newpatlist:
                    if attrs.type == FILEXFER_TYPE_DIRECTORY:
                        await self._match(newpath, attrs, newpatlist)
                else:
                    self._report_match(newpath, attrs)

    async def _match(self, path: bytes, attrs: SFTPAttrs,
                     patlist: SFTPPathList) -> None:
        """Recursively match against a glob pattern"""

        pattern = patlist[0]

        if isinstance(pattern, list):
            await self._match_exact(path, pattern, patlist)
        else:
            await self._match_pattern(path, attrs, pattern, patlist)

    async def match(self, pattern: bytes,
                    error_handler: SFTPErrorHandler = None,
                    sftp_version = MIN_SFTP_VERSION) -> Sequence[SFTPName]:
        """Match against a glob pattern"""

        self._new_matches = []
        self._matched = False

        path, patlist = self._split(pattern)

        try:
            attrs = await self._stat(path or b'.')

            if attrs:
                if patlist:
                    if attrs.type == FILEXFER_TYPE_DIRECTORY:
                        await self._match(path, attrs, patlist)
                elif path:
                    self._report_match(path, attrs)

            if pattern and not self._matched:
                exc = SFTPNoSuchPath if sftp_version >= 4 else SFTPNoSuchFile
                raise exc('No matches found')
        except (OSError, SFTPError) as exc:
            setattr(exc, 'srcpath', pattern)

            if error_handler:
                error_handler(exc)
            else:
                raise

        return self._new_matches
