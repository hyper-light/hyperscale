import asyncio
import io
import pathlib
import posixpath
import time
from collections import deque
from typing import Deque, Literal

from hyperscale.core.engines.client.ssh.protocol.ssh.constants import (
    ACE4_READ_DATA,
    ACE4_APPEND_DATA,
    ACE4_READ_ATTRIBUTES,
    FXF_READ,
    FXF_APPEND,
    FXF_OPEN_EXISTING,
    FXF_APPEND_DATA,
    FILEXFER_ATTR_DEFINED_V4,
    FILEXFER_TYPE_SYMLINK,
    FILEXFER_TYPE_REGULAR,
    FILEXFER_TYPE_DIRECTORY,
    FILEXFER_TYPE_UNKNOWN,
    FXRP_NO_CHECK,
    FXRP_STAT_IF_EXISTS,
)

from .models import (
    FileAttributes,
    FileSet,
    ResultFileType,
    TransferResult,
)
from .models.transfer import Transfer
from .protocols.file import File
from .protocols.sftp import (
    SFTPStatFunc,
    SFTPClientFileOrPath,
    SFTPAttrs,
    SFTPName,
    SFTPGlob,
    SFTPBadMessage,
    SFTPNoSuchFile,
    SFTPNoSuchPath,
    SFTPPermissionDenied,
    SFTPFileAlreadyExists,
    SFTPFailure,
    SFTPClientFile,
    SFTPOpUnsupported,
    SFTPError,
    SFTPNotADirectory,
    SFTPFileIsADirectory,
    SFTPFSProtocol,
    SFTPLimits,
    SFTPFileCopier,
    SFTPVFSAttrs,
    MAX_SFTP_READ_LEN,
    mode_to_pflags,
    utime_to_attrs,
    tuple_to_float_sec,
    tuple_to_nsec,
    valid_attr_flags,
    SFTPClientHandler,
    LocalFS,
    SSHPacket,
)



class SFTPCommand:

    def __init__(
        self,
        handler: SFTPClientHandler,
        local_fs: LocalFS,
        loop: asyncio.AbstractEventLoop,
        base_directory: str | None = None,
        path_encoding: str = 'utf-8',
    ):
        
        if base_directory:
            base_directory: bytes = base_directory.encode(encoding=path_encoding)

        self._base_directory: bytes | None = base_directory

        self._handler = handler
        self._path_encoding = path_encoding
        self._depth_sem = asyncio.Semaphore(value=128)
        self._local_fs = local_fs
        self._loop = loop

    @property
    def version(self) -> int:
        """SFTP version associated with this SFTP session"""

        return self._handler.version

    @property
    def limits(self) -> SFTPLimits:
        """:class:`SFTPLimits` associated with this SFTP session"""

        return self._handler.limits

    @property
    def supports_remote_copy(self) -> bool:
        """Return whether or not SFTP remote copy is supported"""

        return self._handler.supports_copy_data

    @staticmethod
    def basename(path: bytes) -> bytes:
        """Return the final component of a POSIX-style path"""

        return posixpath.basename(path)

    def encode(self, path: str | pathlib.PurePath) -> bytes:
        """Encode path name using configured path encoding

           This method has no effect if the path is already bytes.

        """

        if isinstance(path, pathlib.PurePath):
            path = str(path)

        if isinstance(path, str):
            path = path.encode(self._path_encoding)

        return path

    def decode(self, path: bytes, want_string: bool = True) -> bytes | str:
        """Decode path name using configured path encoding

           This method has no effect if want_string is set to `False`.

        """

        if want_string:
            return path.decode(self._path_encoding)
        
        return path

    def compose_path(
        self,
        path: str | pathlib.PurePath,
        parent: bytes | None = None,
    ) -> bytes:
        """Compose a path

           If parent is not specified, return a path relative to the
           current remote working directory.

        """

        if parent is None and self._base_directory:
            parent = self._base_directory


        path = self.encode(path)

        return posixpath.join(parent, path) if parent else path

    async def _type(
        self,
        path: str | pathlib.PurePath,
        statfunc: SFTPStatFunc | None = None,
    ) -> int:
        """Return the file type of a remote path, or FILEXFER_TYPE_UNKNOWN
           if it can't be accessed"""

        if statfunc is None:
            statfunc = self.stat

        try:
            return (await statfunc(path)).type
        except (SFTPNoSuchFile, SFTPNoSuchPath, SFTPPermissionDenied):
            return FILEXFER_TYPE_UNKNOWN

    async def get(
        self,
        files: list[str | pathlib.PurePath],
        srcfs: SFTPFSProtocol,
        recurse: bool = False,
        follow_symlinks: bool = False,
    ):

        transferred: dict[bytes, TransferResult] = {}
        
        found = await asyncio.gather(*[
            self._stat_remote_paths(path, srcfs) for path in files
        ])


        while len(found):
            (
                srcpath,
                attributes,
            ) = found.pop()

            start = time.monotonic()

            filetype = attributes.type

            if follow_symlinks and filetype == FILEXFER_TYPE_SYMLINK:

                srcattrs = await self._handler.stat(
                    srcpath,
                    FILEXFER_ATTR_DEFINED_V4,
                    follow_symlinks=follow_symlinks,
                )

                attributes = FileAttributes.from_sftp_attrs(srcattrs)
                filetype = srcattrs.type

            if filetype == FILEXFER_TYPE_DIRECTORY and recurse:

                for srcname in await self._scandir(srcpath):
                    filename: bytes = srcname.filename

                    if filename in (b'.', b'..'):
                        continue

                    srcfile = posixpath.join(srcpath, filename)

                    found.append((
                        srcfile,
                        FileAttributes.from_sftp_attrs(srcname.attrs),
                    ))

                transferred[srcpath] = TransferResult(
                    file_path=srcpath,
                    file_type="DIRECTORY",
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start
                    
                )

            elif filetype == FILEXFER_TYPE_DIRECTORY:
                transferred[srcpath] = TransferResult(
                    file_path=srcpath,
                    file_type="DIRECTORY",
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start
                    
                )

            elif filetype == FILEXFER_TYPE_SYMLINK:
                targetpath = await srcfs.readlink(srcpath)
                
                transferred[targetpath] = TransferResult(
                    file_path=targetpath,
                    file_type="SYMLINK",
                    file_transfer_elapsed=time.monotonic() - start
                )
                
            else:
     
                pflags, _ = mode_to_pflags('rb')
                handle = await self._handler.open(srcpath, pflags, attributes)

                src = SFTPClientFile(
                    srcpath,
                    self._handler,
                    handle,
                    bool(pflags & FXF_APPEND),
                    None,
                    'strict',
                    0,
                    -1,
                )

                data = await src.read()
                
                transferred[srcpath] = TransferResult(
                    file_data=srcpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start,
                )
            
        return transferred

    async def put(
        self,
        files: list[
            tuple[
                str | pathlib.PurePath, 
                FileAttributes,
                bytes | None,
            ],
        ],
        follow_symlinks: bool = False,
    ):

        filepath_data_tuples: list[tuple[bytes, FileAttributes, bytes | None]] = []

        for path, attrs, data in files:

            if isinstance(path, pathlib.Path):
                path = str(path)

            dstpath: bytes = path.encode(encoding=self._path_encoding)

            if self._base_directory and self._base_directory not in dstpath:
                dstpath = posixpath.join(self._base_directory, dstpath)

            filepath_data_tuples.append((
                dstpath, 
                attrs,
                data,
            ))

        transferred: dict[bytes, TransferResult] = {}

        for dstpath, attrs, data in filepath_data_tuples:

            start = time.monotonic()

            filetype = attrs.type
            
            if filetype == FILEXFER_TYPE_DIRECTORY:
                await self._make_directories(
                    dstpath,
                    attrs.to_sftp_attrs(),
                )

                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="DIRECTORY",
                    file_attribues=attrs,
                    file_transfer_elapsed=time.monotonic() - start
                )

            elif filetype == FILEXFER_TYPE_SYMLINK:
                
                attrs = await self._handler.stat(
                    dstpath,
                    FILEXFER_ATTR_DEFINED_V4,
                    follow_symlinks=follow_symlinks,
                )


                await self._handler.symlink(data, dstpath)

                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="SYMLINK",
                    file_attribues=attrs,
                    file_transfer_elapsed=time.monotonic() - start
                )

            else:
                pflags, _ = mode_to_pflags('wb')
                handle = await self._handler.open(
                    dstpath,
                    pflags,
                    block_size=0,
                )


                handle = await self._handler.open(dstpath, pflags, attrs)

                dst = SFTPClientFile(
                    self._handler,
                    handle,
                    bool(pflags & FXF_APPEND),
                    None,
                    'strict',
                    0,
                    -1,
                )

                await dst.write(data, 0)

                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attrs,
                    file_transfer_elapsed=time.monotonic() - start
                )

        return transferred

    async def copy(
        self,
        files: list[str | pathlib.PurePath],
        srcfs: SFTPClientHandler,
        recurse: bool = False,
        follow_symlinks: bool = False,
    ):


        transferred: dict[bytes, TransferResult] = {}
        found: list[tuple[bytes, bytes, FileAttributes]] = await asyncio.gather(*[
            self._stat_copy_paths(
                source_path,
                srcfs,
            )
            for source_path in files
        ])


        while len(found):
            (
                srcpath,
                dstpath,
                attributes,
            ) = found.pop()

            start = time.monotonic()

            filetype = attributes.type

            if follow_symlinks and filetype == FILEXFER_TYPE_SYMLINK:
                srcattrs = await self._handler.stat(
                    srcpath,
                    FILEXFER_ATTR_DEFINED_V4,
                    follow_symlinks=follow_symlinks,
                )

                attributes = FileAttributes.from_sftp_attrs(srcattrs)
                filetype = attributes.type

            if filetype == FILEXFER_TYPE_DIRECTORY and recurse:

                await self._make_directories(
                    dstpath,
                    attributes.to_sftp_attrs(),
                )

                for srcname in await self._scandir(srcpath):
                    filename: bytes = srcname.filename

                    if filename in (b'.', b'..'):
                        continue

                    srcfile = posixpath.join(srcpath, filename)
                    dstfile = posixpath.join(dstpath, filename)

                    found.append((
                        srcfile,
                        dstfile,
                        FileAttributes.from_sftp_attrs(srcname.attrs),
                    ))
                
                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="DIRECTORY",
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start
                    
                )

            elif filetype == FILEXFER_TYPE_DIRECTORY:

                await self._make_directories(
                    dstpath,
                    attributes.to_sftp_attrs(),
                )

                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="DIRECTORY",
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start
                    
                )

            elif filetype == FILEXFER_TYPE_SYMLINK:
                targetpath = await srcfs.readlink(srcpath)
                await self._handler.symlink(srcpath, targetpath)
                
                transferred[targetpath] = TransferResult(
                    file_path=targetpath,
                    file_type="SYMLINK",
                    file_transfer_elapsed=time.monotonic() - start
                )
                
            else:
     
                pflags, _ = mode_to_pflags('rb')
                handle = await self._handler.open(srcpath, pflags, attributes)

                src = SFTPClientFile(
                    srcpath,
                    self._handler,
                    handle,
                    bool(pflags & FXF_APPEND),
                    None,
                    'strict',
                    0,
                    -1,
                )

                data = await src.read()
                
                pflags, _ = mode_to_pflags('wb')
                handle = await self._handler.open(dstpath, pflags, attributes)

                dst = SFTPClientFile(
                    self._handler,
                    handle,
                    bool(pflags & FXF_APPEND),
                    None,
                    'strict',
                    0,
                    -1,
                )

                await dst.write(data, 0)
                
                transferred[dstpath] = TransferResult(
                    file_data=dstpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start,
                )
            
        return transferred
    
    async def mget(
        self,
        files: list[str | pathlib.PurePath],
        srcfs: SFTPFSProtocol,
        recurse: bool = False,
        follow_symlinks: bool = False,
    ):
        """Download remote files with glob pattern match

           This method downloads files and directories from the remote
           system matching one or more glob patterns.

           The arguments to this method are identical to the :meth:`get`
           method, except that the remote paths specified can contain
           wildcard patterns.

        """

        glob = SFTPGlob(srcfs, len(files) > 1)
        discovered = await asyncio.gather(*[
            self._glob_remote_paths(
                path,
                srcfs,
                glob,
            ) for path in files
        ])

        found: list[tuple[bytes, FileAttributes]] = []

        for discovered_paths in discovered:
            found.extend(discovered_paths)

        transferred: dict[bytes, TransferResult] = {}

        while len(found):
            (
                srcpath,
                attributes,
            ) = found.pop()

            start = time.monotonic()

            filetype = attributes.type

            if follow_symlinks and filetype == FILEXFER_TYPE_SYMLINK:

                srcattrs = await self._handler.stat(
                    srcpath,
                    FILEXFER_ATTR_DEFINED_V4,
                    follow_symlinks=follow_symlinks,
                )

                attributes = FileAttributes.from_sftp_attrs(srcattrs)
                filetype = srcattrs.type

            if filetype == FILEXFER_TYPE_DIRECTORY and recurse:

                for srcname in await self._scandir(srcpath):
                    filename: bytes = srcname.filename

                    if filename in (b'.', b'..'):
                        continue

                    srcfile = posixpath.join(srcpath, filename)

                    found.append((
                        srcfile,
                        FileAttributes.from_sftp_attrs(srcname.attrs),
                    ))

                transferred[srcpath] = TransferResult(
                    file_path=srcpath,
                    file_type="DIRECTORY",
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start
                    
                )

            elif filetype == FILEXFER_TYPE_DIRECTORY:
                transferred[srcpath] = TransferResult(
                    file_path=srcpath,
                    file_type="DIRECTORY",
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start
                    
                )

            elif filetype == FILEXFER_TYPE_SYMLINK:
                targetpath = await srcfs.readlink(srcpath)
                
                transferred[targetpath] = TransferResult(
                    file_path=targetpath,
                    file_type="SYMLINK",
                    file_transfer_elapsed=time.monotonic() - start
                )
                
            else:
     
                pflags, _ = mode_to_pflags('rb')
                handle = await self._handler.open(srcpath, pflags, attributes)

                src = SFTPClientFile(
                    srcpath,
                    self._handler,
                    handle,
                    bool(pflags & FXF_APPEND),
                    None,
                    'strict',
                    0,
                    -1,
                )

                data = await src.read()
                
                transferred[srcpath] = TransferResult(
                    file_data=srcpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start,
                )
            
        return transferred

    async def mput(
        self,
        files: list[FileSet],
        follow_symlinks: bool = False,
     ):

        loaded = await asyncio.gather(*[
            file_set.load(
                self._loop,
                self._path_encoding,
            ) for file_set in files
        ])

        found: list[tuple[bytes, FileAttributes, bytes | None]] = []
        for file_set in loaded:
            found.extend(file_set)

        transferred: dict[bytes, TransferResult] = {}

        for dstpath, attrs, data in found:

            start = time.monotonic()

            filetype = attrs.type
            
            if filetype == FILEXFER_TYPE_DIRECTORY:
                await self._make_directories(
                    dstpath,
                    attrs.to_sftp_attrs(),
                )

                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="DIRECTORY",
                    file_attribues=attrs,
                    file_transfer_elapsed=time.monotonic() - start
                )

            elif filetype == FILEXFER_TYPE_SYMLINK:
                
                attrs = await self._handler.stat(
                    dstpath,
                    FILEXFER_ATTR_DEFINED_V4,
                    follow_symlinks=follow_symlinks,
                )


                await self._handler.symlink(data, dstpath)

                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="SYMLINK",
                    file_attribues=attrs,
                    file_transfer_elapsed=time.monotonic() - start
                )

            else:
                pflags, _ = mode_to_pflags('wb')
                handle = await self._handler.open(
                    dstpath,
                    pflags,
                    block_size=0,
                )


                handle = await self._handler.open(dstpath, pflags, attrs)

                dst = SFTPClientFile(
                    self._handler,
                    handle,
                    bool(pflags & FXF_APPEND),
                    None,
                    'strict',
                    0,
                    -1,
                )

                await dst.write(data, 0)


                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attrs,
                    file_transfer_elapsed=time.monotonic() - start
                )

        return transferred

    async def mcopy(
        self,
        files: list[str | pathlib.PurePath],
        srcfs: SFTPClientHandler,
        recurse: bool = False,
        follow_symlinks: bool = False,
    ):
        
        glob = SFTPGlob(srcfs, len(files) > 1)
        discovered = await asyncio.gather(*[
            self._glob_remote_copy_paths(
                source_path,
                srcfs,
                glob,
            )
            for source_path in files
        ])

        found: list[tuple[bytes, bytes, FileAttributes]] = []

        for discovered_set in discovered:
            found.extend(discovered_set)

        transferred: dict[bytes, TransferResult] = {}

        while len(found):
            (
                srcpath,
                dstpath,
                attributes,
            ) = found.pop()

            start = time.monotonic()

            filetype = attributes.type

            if follow_symlinks and filetype == FILEXFER_TYPE_SYMLINK:
                srcattrs = await self._handler.stat(
                    srcpath,
                    FILEXFER_ATTR_DEFINED_V4,
                    follow_symlinks=follow_symlinks,
                )

                attributes = FileAttributes.from_sftp_attrs(srcattrs)
                filetype = attributes.type

            if filetype == FILEXFER_TYPE_DIRECTORY and recurse:

                await self._make_directories(
                    dstpath,
                    attributes.to_sftp_attrs(),
                )

                for srcname in await self._scandir(srcpath):
                    filename: bytes = srcname.filename

                    if filename in (b'.', b'..'):
                        continue

                    srcfile = posixpath.join(srcpath, filename)
                    dstfile = posixpath.join(dstpath, filename)

                    found.append((
                        srcfile,
                        dstfile,
                        FileAttributes.from_sftp_attrs(srcname.attrs),
                    ))
                
                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="DIRECTORY",
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start
                    
                )

            elif filetype == FILEXFER_TYPE_DIRECTORY:

                await self._make_directories(
                    dstpath,
                    attributes.to_sftp_attrs(),
                )

                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="DIRECTORY",
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start
                    
                )

            elif filetype == FILEXFER_TYPE_SYMLINK:
                targetpath = await srcfs.readlink(srcpath)
                await self._handler.symlink(srcpath, targetpath)
                
                transferred[targetpath] = TransferResult(
                    file_path=targetpath,
                    file_type="SYMLINK",
                    file_transfer_elapsed=time.monotonic() - start
                )
                
            else:
     
                pflags, _ = mode_to_pflags('rb')
                handle = await self._handler.open(srcpath, pflags, attributes)

                src = SFTPClientFile(
                    srcpath,
                    self._handler,
                    handle,
                    bool(pflags & FXF_APPEND),
                    None,
                    'strict',
                    0,
                    -1,
                )

                data = await src.read()
                
                pflags, _ = mode_to_pflags('wb')
                handle = await self._handler.open(dstpath, pflags, attributes)

                dst = SFTPClientFile(
                    self._handler,
                    handle,
                    bool(pflags & FXF_APPEND),
                    None,
                    'strict',
                    0,
                    -1,
                )

                await dst.write(data, 0)
                
                transferred[dstpath] = TransferResult(
                    file_data=dstpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start,
                )
            
        return transferred
        
    async def _stat_copy_paths(
        self,
        source_path: str,
        srcfs: FileAttributes,
    ):
        if isinstance(source_path, pathlib.Path):
                source_path = str(source_path)

        srcpath: bytes = source_path.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in srcpath:
            srcpath = posixpath.join(self._base_directory, srcpath)


        srcpath, srcattrs = await self._stat_remote_paths(source_path, srcfs)

        return (
            srcpath,
            srcpath,
            srcattrs,
        )
    
    async def _stat_remote_paths(
        self,
        path: str | pathlib.Path,
        srcfs: SFTPFSProtocol,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        srcpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in srcpath:
            srcpath = posixpath.join(self._base_directory, srcpath)

        srcpath = srcfs.encode(srcpath)

        attributes: FileAttributes | None = None

        srcattrs = await self._handler.stat(
            srcpath,
            FILEXFER_ATTR_DEFINED_V4,
            follow_symlinks=True,
        )
        
        attributes = FileAttributes.from_sftp_attrs(srcattrs)

        return (
            srcpath,
            attributes,
        )
    
    async def _glob_remote_paths(
        self,
        path: str | pathlib.Path,
        srcfs: SFTPFSProtocol,
        glob: SFTPGlob,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        srcpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in srcpath:
            srcpath = posixpath.join(self._base_directory, srcpath)

        srcpath = srcfs.encode(srcpath)
        
        matches = await glob.match(
            srcfs.encode(srcpath),
            None,
            self.version,
        )

        srcpaths: list[tuple[bytes, FileAttributes]] = []
        for match in matches:
            srcpath = match.filename
            if isinstance(srcpath, str):
                srcpath = srcpath.encode(encoding=self._path_encoding)

            if match.longname and isinstance(match.longname, str):
                srcpath = match.longname.encode(encoding=self._path_encoding)

            elif match.longname:
                srcpath = match.longname

            srcpaths.append((
                srcpath,
                FileAttributes.from_sftp_attrs(match.attrs)
            ))

        return srcpaths
    
    async def _glob_remote_copy_paths(
        self,
        path: str | pathlib.Path,
        srcfs: SFTPFSProtocol,
        glob: SFTPGlob,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        srcpath = srcfs.encode(path)

        if self._base_directory and self._base_directory not in srcpath:
            srcpath = posixpath.join(self._base_directory, srcpath)
        
        matches = await glob.match(
            srcfs.encode(srcpath),
            None,
            self.version,
        )

        srcpaths: list[tuple[bytes, bytes, FileAttributes]] = []
        for match in matches:
            srcpath = match.filename
            if match.longname and isinstance(match.longname, str):
                srcpath = match.longname.encode(encoding=self._path_encoding)

            elif match.longname:
                srcpath = match.longname

            srcpaths.append((
                srcpath,
                srcpath,
                FileAttributes.from_sftp_attrs(match.attrs)
            ))

        return srcpaths

    async def remote_copy(
        self,
        src: SFTPClientFileOrPath,
        dst: SFTPClientFileOrPath,
        transfer: Transfer,
        src_offset: int = 0,
        src_length: int = 0,
        dst_offset: int = 0,
    ):

        if isinstance(src, (bytes, str, pathlib.PurePath)):
            src: SFTPClientFile = await self.open(src, 'rb', block_size=0)

        if isinstance(dst, (bytes, str, pathlib.PurePath)):
            dst: SFTPClientFile = await self.open(dst, 'wb', block_size=0)

        result =  await self._handler.copy_data(
            src.handle, 
            src_offset, 
            src_length,
            dst.handle,
            dst_offset,
        )
        
        if isinstance(result, SFTPAttrs):
            transfer.update({
                "file_type": TransferResult.to_file_type(result.type),
                "file_attributes": TransferResult.to_attributes(result),
            })
            
        elif isinstance(result, SSHPacket):
            transfer.update({
                "file_data": transfer.get("file_data", b"") + result.data
            })

        elif isinstance(result, tuple) and isinstance(result[0], bytes):
            transfer.update({
                "file_data": result[0],
                "file_transfer_at_end": result[1],
            })
        
        elif isinstance(result, tuple):
            listing = transfer.get("file_listing", [])
            listing.extend([
                {
                    "file_path": record.filename,
                    "file_type": TransferResult.to_file_type(record.attrs.type),
                    "file_attributes": TransferResult.to_attributes(record.attrs),
                    "file_transfer_at_end": result[1],

                } for record in result[0] if isinstance(SFTPName)
            ])

            transfer.update({
                "file_listing": listing,
                "file_transfer_at_end": result[1],
            })
        
        else:
            transfer.update({
                "file_path": dst.path,
                "file_type": FILEXFER_TYPE_UNKNOWN,
                "file_data": result,
            })

        return transfer
            
    async def glob(
        self,
        patterns: list[str],
        srcfs: SFTPFSProtocol,
    ):
        glob = SFTPGlob(srcfs, len(patterns) > 1)

        start = time.monotonic()

        discovered = await asyncio.gather(*[
            self._glob_remote_paths(
                pattern,
                srcfs,
                glob,
            )
            for pattern in patterns
        ])
        
        
        found: list[TransferResult] = []

        for discovered_set in discovered:
            found.extend([
                TransferResult(
                    file_path=srcpath,
                    file_attribues=attrs,
                    file_type=TransferResult.to_file_type(attrs.type),
                    file_transfer_elapsed=time.monotonic() - start,
                )
                for srcpath, attrs in discovered_set
            ])

        return found

    async def glob_sftpname(
        self,
        patterns: str | pathlib.PurePath,
        srcfs: SFTPFSProtocol,
    ):
        glob = SFTPGlob(srcfs, len(patterns) > 1)

        start = time.monotonic()

        discovered = await asyncio.gather(*[
            self._glob_remote_paths(
                pattern,
                srcfs,
                glob,
            )
            for pattern in patterns
        ])

        found: list[TransferResult] = []

        for discovered_set in discovered:
            found.extend([
                TransferResult(
                    file_path=srcpath,
                    file_attribues=attrs,
                    file_type=TransferResult.to_file_type(attrs.type),
                    file_transfer_elapsed=time.monotonic() - start,
                )
                for srcpath, attrs in discovered_set
            ])

        return found

    async def makedirs(
        self,
        files: list[
            tuple[
                str | pathlib.PurePath, 
                FileAttributes,
            ],
        ],
        exist_ok: bool = False,
    ):

        filepath_data_tuples: list[tuple[bytes, FileAttributes, bytes | None]] = []

        for path, attrs in files:

            if isinstance(path, pathlib.Path):
                path = str(path)

            dstpath: bytes = path.encode(encoding=self._path_encoding)

            if self._base_directory and self._base_directory not in dstpath:
                dstpath = posixpath.join(self._base_directory, dstpath)

            filepath_data_tuples.append((
                dstpath, 
                attrs,
            ))

        transferred: dict[bytes, TransferResult] = {}

        for dstpath, attrs in filepath_data_tuples:

            sftp_attrs = attrs.to_sftp_attrs()
            start = time.monotonic()

            await self._make_directories(
                dstpath,
                sftp_attrs,
                exist_ok=exist_ok,
            )
            transferred[dstpath] = TransferResult(
                file_path=dstpath,
                file_attribues=attrs,
                file_transfer_elapsed=time.monotonic() - start,
            )
        
        return path


    async def _make_directories(
        self,
        path: bytes,
        attrs: SFTPAttrs,
        exist_ok: bool = False,
    ):
        curpath = b'/' if posixpath.isabs(path) else b''
        parts = path.split(b'/')
        last = len(parts) - 1

        exists = True

        for i, part in enumerate(parts):
            curpath = posixpath.join(curpath, part)

            try:
                path = self.compose_path(path)
                await self._handler.mkdir(path, attrs)
                exists = False

            except (SFTPFailure, SFTPFileAlreadyExists):
                filetype = await self._type(curpath)

                if filetype != FILEXFER_TYPE_DIRECTORY:
                    curpath_str = curpath.decode('utf-8', 'backslashreplace')

                    exc = SFTPNotADirectory if self.version >= 6 \
                        else SFTPFailure

                    raise exc(f'{curpath_str} is not a directory') from None
            except SFTPPermissionDenied:
                if i == last:
                    raise

            if exists and not exist_ok:
                exc = SFTPFileAlreadyExists if self.version >= 6 else SFTPFailure

                raise exc(curpath.decode('utf-8', 'backslashreplace') +
                        ' already exists')

        return path

    async def rmtree(
        self, 
        patterns: list[str],
        srcfs: SFTPFSProtocol,
    ):
        glob = SFTPGlob(srcfs, len(patterns) > 1)

        start = time.monotonic()

        discovered = await asyncio.gather(*[
            self._glob_remote_paths(
                pattern,
                srcfs,
                glob,
            )
            for pattern in patterns
        ])

        found: Deque[tuple[bytes, FileAttributes]] = deque()
        for discovered_set in discovered:
            found.extend([
                (path, attrs)
                for path, attrs in discovered_set
                if attrs.type != FILEXFER_TYPE_SYMLINK
            ])

        candidates: list[tuple[bytes, FileAttributes]] = []

        while len(found):
            (
                dstpath,
                attrs,
            ) = found.popleft()

            if attrs.type == FILEXFER_TYPE_DIRECTORY:
                for entry in await self._scandir(dstpath):
                    filename: bytes = entry.filename

                    if filename in (b'.', b'..'):
                        continue

                    filepath = posixpath.join(dstpath, filename)
                    
                    if entry.attrs.type == FILEXFER_TYPE_DIRECTORY:
                        found.append((
                            filepath,
                            entry.attrs,
                        ))

                    candidates.append((
                        filepath,
                        FileAttributes.from_sftp_attrs(entry.attrs),
                    ))

            else:
                candidates.append((
                    dstpath,
                    attrs,
                ))

        tasks = []
        for path, attrs, in candidates:
            
            if attrs.type == FILEXFER_TYPE_DIRECTORY:
                tasks.append(asyncio.ensure_future(
                    self._handler.rmdir(path),
                ))

            else:
                tasks.append(asyncio.ensure_future(
                    self._handler.remove(path),
                ))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                raise res
            
        return [
            TransferResult(
                file_path=path,
                file_attribues=attrs,
                file_transfer_elapsed=time.monotonic() - start,
            ) for path, attrs in candidates
        ]

    async def open(
        self,
        path: str | pathlib.PurePath,
        pflags_or_mode: int | str = FXF_READ,
        attrs: SFTPAttrs = SFTPAttrs(),
        encoding: str | None = 'utf-8',
        errors: str = 'strict',
        block_size: int = -1,
        max_requests: int = -1,
    ) -> SFTPClientFile:
        """Open a remote file

           This method opens a remote file and returns an
           :class:`SFTPClientFile` object which can be used to read and
           write data and get and set file attributes.

           The path can be either a `str` or `bytes` value. If it is a
           str, it will be encoded using the file encoding specified
           when the :class:`SFTPClient` was started.

           The following open mode flags are supported:

             ========== ======================================================
             Mode       Description
             ========== ======================================================
             FXF_READ   Open the file for reading.
             FXF_WRITE  Open the file for writing. If both this and FXF_READ
                        are set, open the file for both reading and writing.
             FXF_APPEND Force writes to append data to the end of the file
                        regardless of seek position.
             FXF_CREAT  Create the file if it doesn't exist. Without this,
                        attempts to open a non-existent file will fail.
             FXF_TRUNC  Truncate the file to zero length if it already exists.
             FXF_EXCL   Return an error when trying to open a file which
                        already exists.
             ========== ======================================================

           Instead of these flags, a Python open mode string can also be
           provided. Python open modes map to the above flags as follows:

             ==== =============================================
             Mode Flags
             ==== =============================================
             r    FXF_READ
             w    FXF_WRITE | FXF_CREAT | FXF_TRUNC
             a    FXF_WRITE | FXF_CREAT | FXF_APPEND
             x    FXF_WRITE | FXF_CREAT | FXF_EXCL

             r+   FXF_READ | FXF_WRITE
             w+   FXF_READ | FXF_WRITE | FXF_CREAT | FXF_TRUNC
             a+   FXF_READ | FXF_WRITE | FXF_CREAT | FXF_APPEND
             x+   FXF_READ | FXF_WRITE | FXF_CREAT | FXF_EXCL
             ==== =============================================

           Including a 'b' in the mode causes the `encoding` to be set
           to `None`, forcing all data to be read and written as bytes
           in binary format.

           Most applications should be able to use this method regardless
           of the version of the SFTP protocol negotiated with the SFTP
           server. A conversion from the pflags_or_mode values to the
           SFTPv5/v6 flag values will happen automatically. However, if
           an application wishes to set flags only available in SFTPv5/v6,
           the :meth:`open56` method may be used to specify these flags
           explicitly.

           The attrs argument is used to set initial attributes of the
           file if it needs to be created. Otherwise, this argument is
           ignored.

           The block_size argument specifies the size of parallel read and
           write requests issued on the file. If set to `None`, each read
           or write call will become a single request to the SFTP server.
           Otherwise, read or write calls larger than this size will be
           turned into parallel requests to the server of the requested
           size, defaulting to the maximum allowed by the server, or 16 KB
           if the server doesn't advertise limits.

               .. note:: The OpenSSH SFTP server will close the connection
                         if it receives a message larger than 256 KB. So,
                         when connecting to an OpenSSH SFTP server, it is
                         recommended that the block_size be left at its
                         default of using the server-advertised limits.

           The max_requests argument specifies the maximum number of
           parallel read or write requests issued, defaulting to a
           value between 16 and 128 depending on the selected block
           size to avoid excessive memory usage.

           :param path:
               The name of the remote file to open
           :param pflags_or_mode: (optional)
               The access mode to use for the remote file (see above)
           :param attrs: (optional)
               File attributes to use if the file needs to be created
           :param encoding: (optional)
               The Unicode encoding to use for data read and written
               to the remote file
           :param errors: (optional)
               The error-handling mode if an invalid Unicode byte
               sequence is detected, defaulting to 'strict' which
               raises an exception
           :param block_size: (optional)
               The block size to use for read and write requests
           :param max_requests: (optional)
               The maximum number of parallel read or write requests
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type pflags_or_mode: `int` or `str`
           :type attrs: :class:`SFTPAttrs`
           :type encoding: `str`
           :type errors: `str`
           :type block_size: `int` or `None`
           :type max_requests: `int`

           :returns: An :class:`SFTPClientFile` to use to access the file

           :raises: | :exc:`ValueError` if the mode is not valid
                    | :exc:`SFTPError` if the server returns an error

        """

        if isinstance(pflags_or_mode, str):
            pflags, binary = mode_to_pflags(pflags_or_mode)

            if binary:
                encoding = None
        else:
            pflags = pflags_or_mode

        path = self.compose_path(path)
        handle = await self._handler.open(path, pflags, attrs)

        return SFTPClientFile(
            path,
            self._handler,
            handle,
            bool(pflags & FXF_APPEND),
            encoding,
            errors,
            block_size,
            max_requests,
        )

    async def open56(
        self,
        path: str | pathlib.PurePath,
        desired_access: int = ACE4_READ_DATA | ACE4_READ_ATTRIBUTES,
        flags: int = FXF_OPEN_EXISTING,
        attrs: SFTPAttrs = SFTPAttrs(),
        encoding: str = 'utf-8',
        errors: str = 'strict',
        block_size: int = -1,
        max_requests: int = -1,
    ) -> SFTPClientFile:
        """Open a remote file using SFTP v5/v6 flags

           This method is very similar to :meth:`open`, but the pflags_or_mode
           argument is replaced with SFTPv5/v6 desired_access and flags
           arguments. Most applications can continue to use :meth:`open`
           even when talking to an SFTPv5/v6 server and the translation of
           the flags will happen automatically. However, if an application
           wishes to set flags only available in SFTPv5/v6, this method
           provides that capability.

           The following desired_access flags can be specified:

               | ACE4_READ_DATA
               | ACE4_WRITE_DATA
               | ACE4_APPEND_DATA
               | ACE4_READ_ATTRIBUTES
               | ACE4_WRITE_ATTRIBUTES

           The following flags can be specified:

               | FXF_CREATE_NEW
               | FXF_CREATE_TRUNCATE
               | FXF_OPEN_EXISTING
               | FXF_OPEN_OR_CREATE
               | FXF_TRUNCATE_EXISTING
               | FXF_APPEND_DATA
               | FXF_APPEND_DATA_ATOMIC
               | FXF_BLOCK_READ
               | FXF_BLOCK_WRITE
               | FXF_BLOCK_DELETE
               | FXF_BLOCK_ADVISORY (SFTPv6)
               | FXF_NOFOLLOW (SFTPv6)
               | FXF_DELETE_ON_CLOSE (SFTPv6)
               | FXF_ACCESS_AUDIT_ALARM_INFO (SFTPv6)
               | FXF_ACCESS_BACKUP (SFTPv6)
               | FXF_BACKUP_STREAM (SFTPv6)
               | FXF_OVERRIDE_OWNER (SFTPv6)

           At this time, FXF_TEXT_MODE is not supported. Also, servers
           may support only a subset of these flags. For example,
           the AsyncSSH SFTP server doesn't currently support ACLs,
           file locking, or most of the SFTPv6 open flags, but
           support for some of these may be added over time.

           :param path:
               The name of the remote file to open
           :param desired_access: (optional)
               The access mode to use for the remote file (see above)
           :param flags: (optional)
               The access flags to use for the remote file (see above)
           :param attrs: (optional)
               File attributes to use if the file needs to be created
           :param encoding: (optional)
               The Unicode encoding to use for data read and written
               to the remote file
           :param errors: (optional)
               The error-handling mode if an invalid Unicode byte
               sequence is detected, defaulting to 'strict' which
               raises an exception
           :param block_size: (optional)
               The block size to use for read and write requests
           :param max_requests: (optional)
               The maximum number of parallel read or write requests
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type desired_access: int
           :type flags: int
           :type attrs: :class:`SFTPAttrs`
           :type encoding: `str`
           :type errors: `str`
           :type block_size: `int` or `None`
           :type max_requests: `int`

           :returns: An :class:`SFTPClientFile` to use to access the file

           :raises: | :exc:`ValueError` if the mode is not valid
                    | :exc:`SFTPError` if the server returns an error

        """

        path = self.compose_path(path)
        handle = await self._handler.open56(path, desired_access, flags, attrs)

        return SFTPClientFile(
            path,
            self._handler,
            handle,
            bool(
                desired_access 
                & ACE4_APPEND_DATA 
                or
                flags & FXF_APPEND_DATA,
            ),
            encoding,
            errors,
            block_size,
            max_requests,
        )

    async def stat(
        self,
        path: str | pathlib.PurePath,
        *,
        flags = FILEXFER_ATTR_DEFINED_V4,
        follow_symlinks: bool = True,
    ) -> SFTPAttrs:
        """Get attributes of a remote file, directory, or symlink

           This method queries the attributes of a remote file, directory,
           or symlink. If the path provided is a symlink and follow_symlinks
           is `True`, the returned attributes will correspond to the target
           of the link.

           :param path:
               The path of the remote file or directory to get attributes for
           :param flags: (optional)
               Flags indicating attributes of interest (SFTPv4 only)
           :param follow_symlinks: (optional)
               Whether or not to follow symbolic links
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type flags: `int`
           :type follow_symlinks: `bool`

           :returns: An :class:`SFTPAttrs` containing the file attributes

           :raises: :exc:`SFTPError` if the server returns an error

        """

        path = self.compose_path(path)
        return await self._handler.stat(
            path,
            flags,
            follow_symlinks=follow_symlinks,
        )

    async def lstat(
        self,
        path: str | pathlib.PurePath,
        flags = FILEXFER_ATTR_DEFINED_V4,
    ) -> SFTPAttrs:
        """Get attributes of a remote file, directory, or symlink

           This method queries the attributes of a remote file,
           directory, or symlink. Unlike :meth:`stat`, this method
           returns the attributes of a symlink itself rather than
           the target of that link.

           :param path:
               The path of the remote file, directory, or link to get
               attributes for
           :param flags: (optional)
               Flags indicating attributes of interest (SFTPv4 only)
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type flags: `int`

           :returns: An :class:`SFTPAttrs` containing the file attributes

           :raises: :exc:`SFTPError` if the server returns an error

        """

        path = self.compose_path(path)
        return await self._handler.lstat(path, flags)

    async def setstat(
        self,
        path: str | pathlib.PurePath,
        attrs: SFTPAttrs,
        *,
        follow_symlinks: bool = True,
    ):
        """Set attributes of a remote file, directory, or symlink

           This method sets attributes of a remote file, directory, or
           symlink. If the path provided is a symlink and follow_symlinks
           is `True`, the attributes will be set on the target of the link.
           A subset of the fields in `attrs` can be initialized and only
           those attributes will be changed.

           :param path:
               The path of the remote file or directory to set attributes for
           :param attrs:
               File attributes to set
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type attrs: :class:`SFTPAttrs`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        path = self.compose_path(path)

        await self._handler.setstat(
            path,
            attrs,
            follow_symlinks=follow_symlinks,
        )

        return path

    async def statvfs(
        self,
        path: str | pathlib.PurePath,
    ) -> SFTPVFSAttrs:
        """Get attributes of a remote file system

           This method queries the attributes of the file system containing
           the specified path.

           :param path:
               The path of the remote file system to get attributes for
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :returns: An :class:`SFTPVFSAttrs` containing the file system
                     attributes

           :raises: :exc:`SFTPError` if the server doesn't support this
                    extension or returns an error

        """

        path = self.compose_path(path)
        return await self._handler.statvfs(path)

    async def truncate(
        self,
        path: str | pathlib.PurePath,
        size: int,
    ):
        """Truncate a remote file to the specified size

           This method truncates a remote file to the specified size.
           If the path provided is a symbolic link, the target of
           the link will be truncated.

           :param path:
               The path of the remote file to be truncated
           :param size:
               The desired size of the file, in bytes
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type size: `int`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        await self.setstat(path, SFTPAttrs(size=size))

        return path

    async def chown(
        self, 
        path: str | pathlib.PurePath,
        *,
        uid: int = None,
        gid: int = None,
        owner: str = None,
        group: str = None,
        follow_symlinks: bool = True,
    ):
        """Change the owner of a remote file, directory, or symlink

           This method changes the user and group id of a remote file,
           directory, or symlink. If the path provided is a symlink and
           follow_symlinks is `True`, the target of the link will be changed.

           :param path:
               The path of the remote file to change
           :param uid:
               The new user id to assign to the file
           :param gid:
               The new group id to assign to the file
           :param owner:
               The new owner to assign to the file (SFTPv4 only)
           :param group:
               The new group to assign to the file (SFTPv4 only)
           :param follow_symlinks: (optional)
               Whether or not to follow symbolic links
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type uid: `int`
           :type gid: `int`
           :type owner: `str`
           :type group: `str`
           :type follow_symlinks: `bool`

           :raises: :exc:`SFTPError` if the server returns an error

        """
        await self.setstat(
            path,
            SFTPAttrs(
                uid=uid,
                gid=gid,
                owner=owner,
                group=group,
            ),
            follow_symlinks=follow_symlinks,
        )

        return path

    async def chmod(
        self,
        path: str | pathlib.PurePath,
        mode: int,
        *,
        follow_symlinks: bool = True,
    ):
        """Change the permissions of a remote file, directory, or symlink

           This method changes the permissions of a remote file, directory,
           or symlink. If the path provided is a symlink and follow_symlinks
           is `True`, the target of the link will be changed.

           :param path:
               The path of the remote file to change
           :param mode:
               The new file permissions, expressed as an int
           :param follow_symlinks: (optional)
               Whether or not to follow symbolic links
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type mode: `int`
           :type follow_symlinks: `bool`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        await self.setstat(
            path,
            SFTPAttrs(permissions=mode),
            follow_symlinks=follow_symlinks,
        )

        return path

    async def utime(
        self,
        path: str | pathlib.PurePath,
        *,
        times: tuple[float, float] | None = None,
        ns: tuple[int, int] | None = None,
        follow_symlinks: bool = True,
    ):
        """Change the timestamps of a remote file, directory, or symlink

           This method changes the access and modify times of a remote file,
           directory, or symlink. If neither `times` nor '`ns` is provided,
           the times will be changed to the current time.

           If the path provided is a symlink and follow_symlinks is `True`,
           the target of the link will be changed.

           :param path:
               The path of the remote file to change
           :param times: (optional)
               The new access and modify times, as seconds relative to
               the UNIX epoch
           :param ns: (optional)
               The new access and modify times, as nanoseconds relative to
               the UNIX epoch
           :param follow_symlinks: (optional)
               Whether or not to follow symbolic links
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type times: tuple of two `int` or `float` values
           :type ns: tuple of two `int` values
           :type follow_symlinks: `bool`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        await self.setstat(
            path,
            utime_to_attrs(times, ns),
            follow_symlinks=follow_symlinks,
        )

        return path

    async def exists(
        self,
        path: str | pathlib.PurePath,
    ) -> bool:
        """Return if the remote path exists and isn't a broken symbolic link

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        return await self._type(path) != FILEXFER_TYPE_UNKNOWN

    async def lexists(
        self,
        path: str | pathlib.PurePath,
    ) -> bool:
        """Return if the remote path exists, without following symbolic links

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        return await self._type(
            path,
            statfunc=self.lstat,
        ) != FILEXFER_TYPE_UNKNOWN

    async def getatime(
        self,
        path: str | pathlib.PurePath,
    ) -> float | None:
        """Return the last access time of a remote file or directory

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        attrs = await self.stat(path)

        return (
            tuple_to_float_sec(attrs.atime, attrs.atime_ns)
            if attrs.atime is not None else None
        )

    async def getatime_ns(self, path: str | pathlib.PurePath) -> int | None:
        """Return the last access time of a remote file or directory

           The time returned is nanoseconds since the epoch.

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        attrs = await self.stat(path)

        return (
            tuple_to_nsec(attrs.atime, attrs.atime_ns)
            if attrs.atime is not None else None
        )

    async def getcrtime(
        self,
        path: str | pathlib.PurePath,
    ) -> float | None:
        """Return the creation time of a remote file or directory (SFTPv4 only)

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        attrs = await self.stat(path)

        return (
            tuple_to_float_sec(attrs.crtime, attrs.crtime_ns)
            if attrs.crtime is not None else None
        )

    async def getcrtime_ns(
        self,
        path: str | pathlib.PurePath,
    ) -> int | None:
        """Return the creation time of a remote file or directory

           The time returned is nanoseconds since the epoch.

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        attrs = await self.stat(path)

        return (
            tuple_to_nsec(attrs.crtime, attrs.crtime_ns)
            if attrs.crtime is not None else None
        )

    async def getmtime(
        self,
        path: str | pathlib.PurePath,
    ) -> float | None:
        """Return the last modification time of a remote file or directory

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        attrs = await self.stat(path)

        return (
            tuple_to_float_sec(attrs.mtime, attrs.mtime_ns)
            if attrs.mtime is not None else None
        )

    async def getmtime_ns(
        self,
        path: str | pathlib.PurePath,
    ) -> int | None:
        """Return the last modification time of a remote file or directory

           The time returned is nanoseconds since the epoch.

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        attrs = await self.stat(path)

        return (
            tuple_to_nsec(attrs.mtime, attrs.mtime_ns)
            if attrs.mtime is not None else None
        )

    async def getsize(
        self,
        path: str | pathlib.PurePath,
    ) -> int | None:
        """Return the size of a remote file or directory

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        return (
            await self.stat(path)
        ).size

    async def isdir(
        self,
        path: str | pathlib.PurePath,
    ) -> bool:
        """Return if the remote path refers to a directory

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        return await self._type(path) == FILEXFER_TYPE_DIRECTORY

    async def isfile(
        self,
        path: str | pathlib.PurePath,
    ) -> bool:
        """Return if the remote path refers to a regular file

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        return await self._type(path) == FILEXFER_TYPE_REGULAR

    async def islink(
        self,
        path: str | pathlib.PurePath,
    ) -> bool:
        """Return if the remote path refers to a symbolic link

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        return await self._type(path, statfunc=self.lstat) == FILEXFER_TYPE_SYMLINK

    async def remove(
        self,
        path: str | pathlib.PurePath,
    ):
        """Remove a remote file

           This method removes a remote file or symbolic link.

           :param path:
               The path of the remote file or link to remove
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        path = self.compose_path(path)
        await self._handler.remove(path)

        return path

    async def unlink(
        self,
        path: str | pathlib.PurePath,
    ):
        """Remove a remote file (see :meth:`remove`)"""

        await self.remove(path)

        return path

    async def rename(
        self,
        oldpath: str | pathlib.PurePath,
        newpath: str | pathlib.PurePath,
        flags: int = 0,
    ):
        """Rename a remote file, directory, or link

           This method renames a remote file, directory, or link.

           .. note:: By default, this version of rename will not overwrite
                     the new path if it already exists. However, this
                     can be controlled using the `flags` argument,
                     available in SFTPv5 and later. When a connection
                     is negotiated to use an earliler version of SFTP
                     and `flags` is set, this method will attempt to
                     fall back to the OpenSSH "posix-rename" extension
                     if it is available. That can also be invoked
                     directly by calling :meth:`posix_rename`.

           :param oldpath:
               The path of the remote file, directory, or link to rename
           :param newpath:
               The new name for this file, directory, or link
           :param flags: (optional)
               A combination of the `FXR_OVERWRITE`, `FXR_ATOMIC`, and
               `FXR_NATIVE` flags to specify what happens when `newpath`
               already exists, defaulting to not allowing the overwrite
               (SFTPv5 and later)
           :type oldpath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type newpath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type flags: `int`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        oldpath = self.compose_path(oldpath)
        newpath = self.compose_path(newpath)
        await self._handler.rename(oldpath, newpath, flags)

        return newpath

    async def posix_rename(
        self,
        oldpath: str | pathlib.PurePath,
        newpath: str | pathlib.PurePath,
    ):
        """Rename a remote file, directory, or link with POSIX semantics

           This method renames a remote file, directory, or link,
           removing the prior instance of new path if it previously
           existed.

           This method may not be supported by all SFTP servers. If it
           is not available but the server supports SFTPv5 or later,
           this method will attempt to send the standard SFTP rename
           request with the `FXR_OVERWRITE` flag set.

           :param oldpath:
               The path of the remote file, directory, or link to rename
           :param newpath:
               The new name for this file, directory, or link
           :type oldpath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type newpath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server doesn't support this
                    extension or returns an error

        """

        oldpath = self.compose_path(oldpath)
        newpath = self.compose_path(newpath)
        await self._handler.posix_rename(oldpath, newpath)

        return newpath

    async def scandir(
        self,
        path: str | pathlib.PurePath = '.',
    ):
        """Return names and attributes of the files in a remote directory

           This method reads the contents of a directory, returning
           the names and attributes of what is contained there as an
           async iterator. If no path is provided, it defaults to the
           current remote working directory.

           :param path: (optional)
               The path of the remote directory to read
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :returns: An async iterator of :class:`SFTPName` entries, with
                     path names matching the type used to pass in the path

           :raises: :exc:`SFTPError` if the server returns an error

        """

        dirpath = self.compose_path(path)
        return await self._scandir(dirpath)


    async def _scandir(
        self,
        dirpath: bytes,
    ):
        handle = await self._handler.opendir(dirpath)
        at_end = False

        entries: list[SFTPName] = []

        while not at_end:
            names, at_end = await self._handler.readdir(handle)

            for entry in names:
                if isinstance(path, (str, pathlib.PurePath)):
                    entry.filename = self.decode(entry.filename)

                    if entry.longname is not None:
                        entry.longname = self.decode(entry.longname)

                entries.append(entry)

        return entries

    async def readdir(
        self,
        path: str | pathlib.PurePath = '.',
    ) -> list[SFTPName]:
        """Read the contents of a remote directory

           This method reads the contents of a directory, returning
           the names and attributes of what is contained there. If no
           path is provided, it defaults to the current remote working
           directory.

           :param path: (optional)
               The path of the remote directory to read
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :returns: A list of :class:`SFTPName` entries, with path
                     names matching the type used to pass in the path

           :raises: :exc:`SFTPError` if the server returns an error

        """

        return [
            entry
            for entry in await self.scandir(path)
        ]

    async def listdir(
        self,
        path: str | pathlib.PurePath = '.',
    ) -> list[bytes | str]:
        """Read the names of the files in a remote directory

           This method reads the names of files and subdirectories
           in a remote directory. If no path is provided, it defaults
           to the current remote working directory.

           :param path: (optional)
               The path of the remote directory to read
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :returns: A list of file/subdirectory names, as a `str` or `bytes`
                     matching the type used to pass in the path

           :raises: :exc:`SFTPError` if the server returns an error

        """

        names = await self.readdir(path)
        return [
            name.filename
            for name in names
        ]

    async def mkdir(
        self,
        path: str | pathlib.PurePath,
        attrs: SFTPAttrs = SFTPAttrs(),
    ):
        """Create a remote directory with the specified attributes

           This method creates a new remote directory at the
           specified path with the requested attributes.

           :param path:
               The path of where the new remote directory should be created
           :param attrs: (optional)
               The file attributes to use when creating the directory
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type attrs: :class:`SFTPAttrs`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        path = self.compose_path(path)
        await self._handler.mkdir(path, attrs)

        return path

    async def rmdir(
        self,
        path: str | pathlib.PurePath,
    ):
        """Remove a remote directory

           This method removes a remote directory. The directory
           must be empty for the removal to succeed.

           :param path:
               The path of the remote directory to remove
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        path = self.compose_path(path)
        await self._handler.rmdir(path)

        return path

    async def realpath(
        self,
        path: str | pathlib.PurePath,
        *compose_paths: str | pathlib.PurePath,
        check: int = FXRP_NO_CHECK,
    ) -> bytes | str | SFTPName:
        """Return the canonical version of a remote path

           This method returns a canonical version of the requested path.

           :param path: (optional)
               The path of the remote directory to canonicalize
           :param compose_paths: (optional)
               A list of additional paths that the server should compose
               with `path` before canonicalizing it
           :param check: (optional)
               One of `FXRP_NO_CHECK`, `FXRP_STAT_IF_EXISTS`, and
               `FXRP_STAT_ALWAYS`, specifying when to perform a
               stat operation on the resulting path, defaulting to
               `FXRP_NO_CHECK`
           :type path:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type compose_paths:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type check: int

           :returns: The canonical path as a `str` or `bytes`, matching
                     the type used to pass in the path if `check` is set
                     to `FXRP_NO_CHECK`, or an :class:`SFTPName`
                     containing the canonical path name and attributes
                     otherwise

           :raises: :exc:`SFTPError` if the server returns an error

        """

        if compose_paths and isinstance(compose_paths[-1], int):
            check = compose_paths[-1]
            compose_paths = compose_paths[:-1]

        path_bytes = self.compose_path(path)

        if self.version >= 6:
            names, _ = await self._handler.realpath(
                path_bytes,
                *map(
                    self.encode,
                    compose_paths,
                ),
                check=check,
            )

        else:
            for cpath in compose_paths:
                path_bytes = self.compose_path(cpath, path_bytes)

            names, _ = await self._handler.realpath(path_bytes)

        if len(names) > 1:
            raise SFTPBadMessage('Too many names returned')

        if check != FXRP_NO_CHECK:
            if self.version < 6:
                try:
                    names[0].attrs = await self._handler.stat(
                        self.encode(
                            names[0].filename,
                        ),
                        valid_attr_flags[self.version],
                    )
                    
                except SFTPError as err:
                    if check != FXRP_STAT_IF_EXISTS:
                        raise err
                    
                    names[0].attrs = SFTPAttrs(type=FILEXFER_TYPE_UNKNOWN)

            return names[0]
        else:
            return self.decode(
                names[0].filename,
                isinstance(
                    path, 
                    (str, pathlib.PurePath),
                ),
            )

    async def getcwd(self) -> bytes | str:
        """Return the current remote working directory

           :returns: The current remote working directory, decoded using
                     the specified path encoding

           :raises: :exc:`SFTPError` if the server returns an error

        """

        cwd = await self.realpath(b'.')
        return self.decode(cwd)

    async def chdir(
        self,
        path: str | pathlib.PurePath,
    ) -> str:
        """Change the current remote working directory

           :param path:
               The path to set as the new remote working directory
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        return await self.realpath(
            self.encode(path),
        )

    async def readlink(
        self,
        path: str | pathlib.PurePath,
    ) -> bytes | str:
        """Return the target of a remote symbolic link

           This method returns the target of a symbolic link.

           :param path:
               The path of the remote symbolic link to follow
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :returns: The target path of the link as a `str` or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        linkpath = self.compose_path(path)
        names, _ = await self._handler.readlink(linkpath)

        if len(names) > 1:
            raise SFTPBadMessage('Too many names returned')

        return self.decode(
            names[0].filename,
            isinstance(
                path,
                (str, pathlib.PurePath),
            ),
        )

    async def symlink(
        self,
        oldpath: str | pathlib.PurePath,
        newpath: str | pathlib.PurePath,
    ) -> None:
        """Create a remote symbolic link

           This method creates a symbolic link. The argument order here
           matches the standard Python :meth:`os.symlink` call. The
           argument order sent on the wire is automatically adapted
           depending on the version information sent by the server, as
           a number of servers (OpenSSH in particular) did not follow
           the SFTP standard when implementing this call.

           :param oldpath:
               The path the link should point to
           :param newpath:
               The path of where to create the remote symbolic link
           :type oldpath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type newpath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        oldpath = self.encode(oldpath)
        newpath = self.compose_path(newpath)
        await self._handler.symlink(oldpath, newpath)

        return newpath

    async def link(
        self,
        oldpath: str | pathlib.PurePath,
        newpath: str | pathlib.PurePath,
    ) -> None:
        """Create a remote hard link

           This method creates a hard link to the remote file specified
           by oldpath at the location specified by newpath.

           This method may not be supported by all SFTP servers.

           :param oldpath:
               The path of the remote file the hard link should point to
           :param newpath:
               The path of where to create the remote hard link
           :type oldpath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type newpath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`SFTPError` if the server doesn't support this
                    extension or returns an error

        """

        oldpath = self.compose_path(oldpath)
        newpath = self.compose_path(newpath)
        await self._handler.link(oldpath, newpath)

        return newpath

    def exit(self) -> None:
        """Exit the SFTP client session

           This method exits the SFTP client session, closing the
           corresponding channel opened on the server.

        """

        self._handler.exit()

    async def wait_closed(self) -> None:
        """Wait for this SFTP client session to close"""

        await self._handler.wait_closed()
