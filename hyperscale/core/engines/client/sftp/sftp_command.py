import asyncio
import pathlib
import posixpath
import time
from collections import deque
from typing import Deque

from hyperscale.core.engines.client.ssh.protocol.ssh.constants import (
    ACE4_READ_DATA,
    ACE4_APPEND_DATA,
    ACE4_READ_ATTRIBUTES,
    FXF_APPEND,
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
    FileGlob,
    TransferResult,
)
from .protocols.sftp import (
    SFTPStatFunc,
    SFTPGlob,
    SFTPBadMessage,
    SFTPNoSuchFile,
    SFTPNoSuchPath,
    SFTPPermissionDenied,
    SFTPFileAlreadyExists,
    SFTPFailure,
    SFTPClientFile,
    SFTPError,
    SFTPNotADirectory,
    SFTPLimits,
    SFTPVFSAttrs,
    mode_to_pflags,
    utime_to_attrs,
    tuple_to_float_sec,
    tuple_to_nsec,
    valid_attr_flags,
    SFTPClientHandler,
    LocalFS,
)



class SFTPCommand:

    def __init__(
        self,
        handler: SFTPClientHandler,
        local_fs: LocalFS,
        loop: asyncio.AbstractEventLoop,
        base_directory: str | None = None,
        path_encoding: str = 'utf-8',
        copy_handler: SFTPClientHandler | None = None,
    ):
        
        if base_directory:
            base_directory: bytes = base_directory.encode(encoding=path_encoding)

        self._base_directory: bytes | None = base_directory

        self._handler = handler
        self._path_encoding = path_encoding
        self._depth_sem = asyncio.Semaphore(value=128)
        self._local_fs = local_fs
        self._loop = loop
        self._copy_handler = copy_handler

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
        path: str | pathlib.PurePath,
        recurse: bool = False,
        follow_symlinks: bool = False,
        min_version: int = 4,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
        desired_access: int = ACE4_READ_DATA | ACE4_READ_ATTRIBUTES,
    ):
        
        transferred: dict[bytes, TransferResult] = {}
        
        operation_start = time.monotonic()
        remote_path = await self._stat_remote_paths(
            path,
            flags=flags,
        )

        found: Deque[tuple[bytes, FileAttributes]] = deque([remote_path])

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
                    flags,
                    follow_symlinks=follow_symlinks,
                )

                attributes = FileAttributes.from_sftp_attrs(srcattrs)
                filetype = srcattrs.type

            if filetype == FILEXFER_TYPE_DIRECTORY and recurse:

                async for srcname in self._scandir(srcpath):
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
                targetpath = await self._handler.readlink(srcpath)
                
                transferred[targetpath] = TransferResult(
                    file_path=targetpath,
                    file_type="SYMLINK",
                    file_transfer_elapsed=time.monotonic() - start
                )
                
            else:
     
                pflags, _ = mode_to_pflags('rb')

                if min_version < 5:
                    handle = await self._handler.open(srcpath, flags, attributes)
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

                else:

                    handle = await self._handler.open56(srcpath, desired_access, flags, attributes)
                    src = SFTPClientFile(
                        srcpath,
                        self._handler,
                        handle,
                        bool(
                            desired_access 
                            & ACE4_APPEND_DATA 
                            or
                            pflags & FXF_APPEND_DATA,
                        ),
                        None,
                        'strict',
                        0,
                        -1,
                    )

                data = await src.read()
                await src.close()
                
                transferred[srcpath] = TransferResult(
                    file_data=srcpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start,
                )
            
        return (
            time.monotonic() - operation_start,
            transferred,
        )

    async def put(
        self,
        path: str | pathlib.PurePath,
        attributes: FileAttributes,
        data: bytes,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
        min_version: int = 4,
        desired_access: int = ACE4_READ_DATA | ACE4_READ_ATTRIBUTES,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        filetype = attributes.type
        start = time.monotonic()
        
        if filetype == FILEXFER_TYPE_DIRECTORY:
            await self._make_directories(
                dstpath,
                attributes,
            )

            result = TransferResult(
                file_path=dstpath,
                file_type="DIRECTORY",
                file_attribues=attributes,
                file_transfer_elapsed=time.monotonic() - start
            )

        elif filetype == FILEXFER_TYPE_SYMLINK:
            
            srcattrs = await self._handler.stat(
                dstpath,
                flags,
                follow_symlinks=follow_symlinks,
            )


            await self._handler.symlink(data, dstpath)

            result = TransferResult(
                file_path=dstpath,
                file_type="SYMLINK",
                file_attribues=TransferResult.to_attributes(srcattrs),
                file_transfer_elapsed=time.monotonic() - start
            )

        else:
            pflags, _ = mode_to_pflags('wb')

            if min_version < 5:
                handle = await self._handler.open(dstpath, flags, attributes)
                dst = SFTPClientFile(
                    dstpath,
                    self._handler,
                    handle,
                    bool(pflags & FXF_APPEND),
                    None,
                    'strict',
                    0,
                    -1,
                )

            else:
                handle = await self._handler.open56(dstpath, desired_access, flags, attributes)
                dst = SFTPClientFile(
                    dstpath,
                    self._handler,
                    handle,
                    bool(
                        desired_access 
                        & ACE4_APPEND_DATA 
                        or
                        pflags & FXF_APPEND_DATA,
                    ),
                    None,
                    'strict',
                    0,
                    -1,
                )

            await dst.write(data, 0)
            await dst.close()

            result = TransferResult(
                file_path=dstpath,
                file_type="FILE",
                file_data=data,
                file_attribues=attributes,
                file_transfer_elapsed=time.monotonic() - start
            )
    
        return (
            time.monotonic() - start,
            {
                dstpath: result
            },
        )

    async def copy(
        self,
        path: str | pathlib.PurePath,
        recurse: bool = False,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
        min_version: int = 4,
        desired_access: int = ACE4_READ_DATA | ACE4_READ_ATTRIBUTES,
    ):

        transferred: dict[bytes, TransferResult] = {}
        operation_start = time.monotonic()
        remote_path = await self._stat_remote_paths(
            path,
            flags=flags,
        )

        found: Deque[tuple[bytes, FileAttributes]] = deque([remote_path])

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
                    flags,
                    follow_symlinks=follow_symlinks,
                )

                attributes = FileAttributes.from_sftp_attrs(srcattrs)
                filetype = attributes.type

            if filetype == FILEXFER_TYPE_DIRECTORY and recurse:

                await self._make_directories(
                    srcpath,
                    attributes,
                )

                async for srcname in self._scandir(srcpath):
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

                await self._make_directories(
                    srcpath,
                    attributes,
                )

                transferred[srcpath] = TransferResult(
                    file_path=srcpath,
                    file_type="DIRECTORY",
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start
                    
                )

            elif filetype == FILEXFER_TYPE_SYMLINK:
                targetpath = await self._handler.readlink(srcpath)
                await self._handler.symlink(srcpath, targetpath)
                
                transferred[targetpath] = TransferResult(
                    file_path=targetpath,
                    file_type="SYMLINK",
                    file_transfer_elapsed=time.monotonic() - start
                )
                
            else:
     
                pflags, _ = mode_to_pflags('rb')

                if min_version < 5:
                    handle = await self._handler.open(srcpath, flags, attributes)
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

                else:

                    handle = await self._handler.open56(srcpath, desired_access, flags, attributes)
                    src = SFTPClientFile(
                        srcpath,
                        self._handler,
                        handle,
                        bool(
                            desired_access 
                            & ACE4_APPEND_DATA 
                            or
                            pflags & FXF_APPEND_DATA,
                        ),
                        None,
                        'strict',
                        0,
                        -1,
                    )

                data = await src.read()
                await src.close()

                copy_handler = self._copy_handler
                if copy_handler is None:
                    copy_handler = self._handler
                
                pflags, _ = mode_to_pflags('wb')

                if min_version < 5:
                    handle = await self._handler.open(srcpath, flags, attributes)
                    dst = SFTPClientFile(
                        srcpath,
                        copy_handler,
                        handle,
                        bool(pflags & FXF_APPEND),
                        None,
                        'strict',
                        0,
                        -1,
                    )

                else:
                    handle = await self._handler.open56(srcpath, desired_access, flags, attributes)
                    dst = SFTPClientFile(
                        srcpath,
                        copy_handler,
                        handle,
                        bool(
                            desired_access 
                            & ACE4_APPEND_DATA 
                            or
                            pflags & FXF_APPEND_DATA,
                        ),
                        None,
                        'strict',
                        0,
                        -1,
                    )

                await dst.write(data, 0)
                await dst.close()
                
                transferred[srcpath] = TransferResult(
                    file_data=srcpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start,
                )
            
        return (
            time.monotonic() - operation_start,
            transferred
        )
    
    async def mget(
        self,
        pattern: str | pathlib.PurePath,
        recurse: bool = False,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
        min_version: int = 4,
        desired_access: int = ACE4_READ_DATA | ACE4_READ_ATTRIBUTES,
    ):
        """Download remote files with glob pattern match

           This method downloads files and directories from the remote
           system matching one or more glob patterns.

           The arguments to this method are identical to the :meth:`get`
           method, except that the remote paths specified can contain
           wildcard patterns.

        """

        operation_start = time.monotonic()

        glob = SFTPGlob(self._handler, False)
        found = await self._glob_remote_paths(
            pattern,
            glob,
        )

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
                    flags,
                    follow_symlinks=follow_symlinks,
                )

                attributes = FileAttributes.from_sftp_attrs(srcattrs)
                filetype = srcattrs.type

            if filetype == FILEXFER_TYPE_DIRECTORY and recurse:

                async for srcname in self._scandir(srcpath):
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
                targetpath = await self._handler.readlink(srcpath)
                
                transferred[targetpath] = TransferResult(
                    file_path=targetpath,
                    file_type="SYMLINK",
                    file_transfer_elapsed=time.monotonic() - start
                )
                
            else:
     
                pflags, _ = mode_to_pflags('rb')

                if min_version < 5:
                    handle = await self._handler.open(srcpath, flags, attributes)
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

                else:

                    handle = await self._handler.open56(srcpath, desired_access, flags, attributes)
                    src = SFTPClientFile(
                        srcpath,
                        self._handler,
                        handle,
                        bool(
                            desired_access 
                            & ACE4_APPEND_DATA 
                            or
                            pflags & FXF_APPEND_DATA,
                        ),
                        None,
                        'strict',
                        0,
                        -1,
                    )

                data = await src.read()
                await src.close()
                
                transferred[srcpath] = TransferResult(
                    file_data=srcpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start,
                )
            
        return (
            time.monotonic() - operation_start,
            transferred
        )

    async def mput(
        self,
        path: str | pathlib.PurePath,
        attributes: FileAttributes,
        data: bytes | FileGlob,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
        min_version: int = 4,
        desired_access: int = ACE4_READ_DATA | ACE4_READ_ATTRIBUTES,
     ):


        transferred: dict[bytes, TransferResult] = {}

        if isinstance(data, FileGlob):
            found = await data.load(
                self._loop,
                path,
                attributes,
                self._path_encoding,
            )

        else:

            if isinstance(path, pathlib.Path):
                path = str(path)

            dstpath: bytes = path.encode(encoding=self._path_encoding)

            if self._base_directory and self._base_directory not in dstpath:
                dstpath = posixpath.join(self._base_directory, dstpath)

            found = [(
                dstpath,
                attributes,
                data,
            )]
            
        operation_start = time.monotonic()

        for dstpath, attrs, data in found:

            start = time.monotonic()

            filetype = attrs.type
            
            if filetype == FILEXFER_TYPE_DIRECTORY:
                await self._make_directories(
                    dstpath,
                    attrs,
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
                    flags,
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


                if min_version < 5:
                    handle = await self._handler.open(dstpath, flags, attrs)
                    dst = SFTPClientFile(
                        dstpath,
                        self._handler,
                        handle,
                        bool(pflags & FXF_APPEND),
                        None,
                        'strict',
                        0,
                        -1,
                    )

                else:
                    handle = await self._handler.open56(dstpath, desired_access, flags, attrs)
                    dst = SFTPClientFile(
                        dstpath,
                        self._handler,
                        handle,
                        bool(
                            desired_access 
                            & ACE4_APPEND_DATA 
                            or
                            pflags & FXF_APPEND_DATA,
                        ),
                        None,
                        'strict',
                        0,
                        -1,
                    )

                await dst.write(data, 0)
                await dst.close()

                transferred[dstpath] = TransferResult(
                    file_path=dstpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attrs,
                    file_transfer_elapsed=time.monotonic() - start
                )

        return (
            time.monotonic() - operation_start,
            transferred
        )

    async def mcopy(
        self,
        path: str | pathlib.PurePath,
        recurse: bool = False,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
        min_version: int = 4,
        desired_access: int = ACE4_READ_DATA | ACE4_READ_ATTRIBUTES,
    ):
      
        transferred: dict[bytes, TransferResult] = {}
        glob = SFTPGlob(self._handler, False)
        found = await self._glob_remote_paths(
            path,
            glob,
        )

        operation_start = time.monotonic()

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
                    flags,
                    follow_symlinks=follow_symlinks,
                )

                attributes = FileAttributes.from_sftp_attrs(srcattrs)
                filetype = attributes.type

            if filetype == FILEXFER_TYPE_DIRECTORY and recurse:

                await self._make_directories(
                    srcpath,
                    attributes,
                )

                async for srcname in self._scandir(srcpath):
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

                await self._make_directories(
                    srcpath,
                    attributes,
                )

                transferred[srcpath] = TransferResult(
                    file_path=srcpath,
                    file_type="DIRECTORY",
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start
                    
                )

            elif filetype == FILEXFER_TYPE_SYMLINK:
                targetpath = await self._handler.readlink(srcpath)
                await self._handler.symlink(srcpath, targetpath)
                
                transferred[targetpath] = TransferResult(
                    file_path=targetpath,
                    file_type="SYMLINK",
                    file_transfer_elapsed=time.monotonic() - start
                )
                
            else:
     
                pflags, _ = mode_to_pflags('rb')

                if min_version < 5:
                    handle = await self._handler.open(srcpath, flags, attributes)
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

                else:

                    handle = await self._handler.open56(srcpath, desired_access, flags, attributes)
                    src = SFTPClientFile(
                        srcpath,
                        self._handler,
                        handle,
                        bool(
                            desired_access 
                            & ACE4_APPEND_DATA 
                            or
                            pflags & FXF_APPEND_DATA,
                        ),
                        None,
                        'strict',
                        0,
                        -1,
                    )

                data = await src.read()
                await src.close()

                copy_handler = self._copy_handler
                if copy_handler is None:
                    copy_handler = self._handler
                
                pflags, _ = mode_to_pflags('wb')

                if min_version < 5:
                    handle = await self._handler.open(srcpath, flags, attributes)
                    dst = SFTPClientFile(
                        srcpath,
                        copy_handler,
                        handle,
                        bool(pflags & FXF_APPEND),
                        None,
                        'strict',
                        0,
                        -1,
                    )

                else:
                    handle = await self._handler.open56(srcpath, desired_access, flags, attributes)
                    dst = SFTPClientFile(
                        srcpath,
                        copy_handler,
                        handle,
                        bool(
                            desired_access 
                            & ACE4_APPEND_DATA 
                            or
                            pflags & FXF_APPEND_DATA,
                        ),
                        None,
                        'strict',
                        0,
                        -1,
                    )

                await dst.write(data, 0)
                await dst.close()
                
                transferred[srcpath] = TransferResult(
                    file_data=srcpath,
                    file_type="FILE",
                    file_data=data,
                    file_attribues=attributes,
                    file_transfer_elapsed=time.monotonic() - start,
                )
            
        return (
            time.monotonic() - operation_start,
            transferred
        )
    
    async def glob(
        self,
        pattern: str | pathlib.Path,
    ):
        glob = SFTPGlob(self._handler, False)

        start = time.monotonic()

        discovered = await self._glob_remote_paths(
            pattern,
            glob,
        )
    
        elapsed = time.monotonic() - start
        transferred: dict[bytes, TransferResult] = {}

        for discovered_set in discovered:
            transferred.update({
                srcpath: TransferResult(
                    file_path=srcpath,
                    file_attribues=attrs,
                    file_type=TransferResult.to_file_type(attrs.type),
                    file_transfer_elapsed=elapsed,
                )
                for srcpath, attrs in discovered_set
            })

        return (
            time.monotonic() - start,
            transferred,
        )

    async def glob_sftpname(
        self,
        pattern: str | pathlib.PurePath,
    ):
        glob = SFTPGlob(self._handler, False)

        start = time.monotonic()

        discovered = await self._glob_remote_paths(
            pattern,
            glob,
        )

        elapsed = time.monotonic() - start
        transferred: dict[bytes, TransferResult] = {}

        for discovered_set in discovered:
            transferred.update({
                srcpath: TransferResult(
                    file_path=srcpath,
                    file_attribues=attrs,
                    file_type=TransferResult.to_file_type(attrs.type),
                    file_transfer_elapsed=elapsed,
                )
                for srcpath, attrs in discovered_set
            })

        return (
            elapsed,
            transferred,
        )

    async def makedirs(
        self,
        path: str | pathlib.PurePath,
        attributes: FileAttributes,
        exist_ok: bool = False,
    ):

        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        transferred: dict[bytes, TransferResult] = {}
        operation_start = time.monotonic() - start


        sftp_attrs = attributes
        start = time.monotonic()

        await self._make_directories(
            dstpath,
            sftp_attrs,
            exist_ok=exist_ok,
        )
        transferred[dstpath] = TransferResult(
            file_path=dstpath,
            file_attribues=attributes,
            file_transfer_elapsed=time.monotonic() - start,
        )
        
        return (
            time.monotonic() - operation_start,
            transferred,
        )

    async def rmtree(
        self, 
        pattern: str | pathlib.Path,
    ):
        glob = SFTPGlob(self._handler, False)

        start = time.monotonic()

        discovered = await self._glob_remote_paths(
            pattern,
            glob,
        )

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
                async for entry in self._scandir(dstpath):
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
            
        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                path: TransferResult(
                    file_path=path,
                    file_attribues=attrs,
                    file_transfer_elapsed=elapsed,
                ) for path, attrs in candidates
            }
        )
    
    async def _stat_remote_paths(
        self,
        path: str | pathlib.Path,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        srcpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in srcpath:
            srcpath = posixpath.join(self._base_directory, srcpath)

        attributes: FileAttributes | None = None

        srcattrs = await self._handler.stat(
            srcpath,
            flags,
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
        glob: SFTPGlob,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        srcpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in srcpath:
            srcpath = posixpath.join(self._base_directory, srcpath)
        
        matches = await glob.match(
            srcpath,
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

    async def _make_directories(
        self,
        path: bytes,
        attrs: FileAttributes,
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

    async def stat(
        self,
        path: str | pathlib.PurePath,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
        follow_symlinks: bool = True,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.stat(
            dstpath,
            flags,
            follow_symlinks=follow_symlinks,
        )

        elapsed = time.monotonic() - start
        return {
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=TransferResult.to_attributes(attrs),
                    file_transfer_elapsed=elapsed,
                )
            }
        }

    async def lstat(
        self,
        path: str | pathlib.PurePath,
        flags = FILEXFER_ATTR_DEFINED_V4,
    ):

        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.lstat(dstpath, flags)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=TransferResult.to_attributes(attrs),
                    file_transfer_elapsed=elapsed,
                )
            }
        )


    async def setstat(
        self,
        path: str | pathlib.PurePath,
        attributes: FileAttributes,
        *,
        follow_symlinks: bool = True,
    ):

        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        await self._handler.setstat(
            path,
            attributes,
            follow_symlinks=follow_symlinks,
        )
        
        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=attributes,
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def statvfs(
        self,
        path: str | pathlib.PurePath,
    ) -> SFTPVFSAttrs:

        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()
        sftpvfs_attrs = await self._handler.statvfs(path)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=TransferResult.to_filesystem_attributes(sftpvfs_attrs),
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def truncate(
        self,
        path: str | pathlib.PurePath,
        size: int,
    ):
        return await self.setstat(path, FileAttributes(size=size))

    async def chown(
        self, 
        path: str | pathlib.PurePath,
        uid: int = None,
        gid: int = None,
        owner: str = None,
        group: str = None,
        follow_symlinks: bool = True,
    ):
        return await self.setstat(
            path,
            FileAttributes(
                uid=uid,
                gid=gid,
                owner=owner,
                group=group,
            ),
            follow_symlinks=follow_symlinks,
        )

    async def chmod(
        self,
        path: str | pathlib.PurePath,
        mode: int,
        follow_symlinks: bool = True,
    ):
        return await self.setstat(
            path,
            FileAttributes(permissions=mode),
            follow_symlinks=follow_symlinks,
        )

    async def utime(
        self,
        path: str | pathlib.PurePath,
        times: tuple[float, float] | None = None,
        ns: tuple[int, int] | None = None,
        follow_symlinks: bool = True,
    ):
        return await self.setstat(
            path,
            utime_to_attrs(times, ns),
            follow_symlinks=follow_symlinks,
        )

    async def exists(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()
        
        attrs = await self._handler.stat(
            dstpath,
            flags=flags,
            follow_symlinks=follow_symlinks,
        )

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type=TransferResult.to_file_type(attrs.type) if attrs.type != FILEXFER_TYPE_UNKNOWN else None,
                    file_attribues=TransferResult.to_attributes(attrs),
                    file_transfer_elapsed=elapsed,
                ),
            }
        )

    async def lexists(
        self,
        path: str | pathlib.PurePath,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()
        
        attrs = await self._handler.lstat(
            dstpath,
            flags=flags,
        )

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type=TransferResult.to_file_type(attrs.type) if attrs.type != FILEXFER_TYPE_UNKNOWN else None,
                    file_attribues=TransferResult.to_attributes(attrs),
                    file_transfer_elapsed=elapsed,
                ),
            }
        )


    async def getatime(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.stat(
            dstpath,
            flags=flags,
            follow_symlinks=follow_symlinks,
        )

        atime: float | None = None
        if attrs.atime is not None:
            atime = tuple_to_float_sec(attrs.atime, attrs.atime_ns)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=FileAttributes(
                        atime=atime,
                    ),
                    file_transfer_elapsed=elapsed,
                ),
            }
        )

    async def getatime_ns(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        attrs = await self._handler.stat(
            dstpath,
            flags=flags,
            follow_symlinks=follow_symlinks,
        )

        start = time.monotonic()

        atime_ns: float | None = None
        if attrs.atime is not None:
            atime_ns = tuple_to_nsec(attrs.atime, attrs.atime_ns)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=FileAttributes(
                        atime_ns=atime_ns,
                    ),
                    file_transfer_elapsed=elapsed,
                ),
            }
        )


    async def getcrtime(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.stat(
            dstpath,
            flags=flags,
            follow_symlinks=follow_symlinks,
        )

        crtime: float | None = None
        if attrs.crtime is not None:
            crtime = tuple_to_float_sec(attrs.crtime, attrs.crtime_ns)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=FileAttributes(
                        crtime=crtime,
                    ),
                    file_transfer_elapsed=elapsed,
                ),
            }
        )

    async def getcrtime_ns(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        attrs = await self._handler.stat(
            dstpath,
            flags=flags,
            follow_symlinks=follow_symlinks,
        )

        start = time.monotonic()

        crtime_ns: float | None = None
        if attrs.atime is not None:
            crtime_ns = tuple_to_nsec(attrs.crtime, attrs.crtime_ns)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=FileAttributes(
                        crtime_ns=crtime_ns,
                    ),
                    file_transfer_elapsed=elapsed,
                )
            }
        )
    
    async def getmtime(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.stat(
            dstpath,
            flags=flags,
            follow_symlinks=follow_symlinks,
        )

        mtime: float | None = None
        if attrs.crtime is not None:
            mtime = tuple_to_float_sec(attrs.mtime, attrs.mtime_ns)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=FileAttributes(
                        mtime=mtime,
                    ),
                    file_transfer_elapsed=elapsed,
                )
            }
        )
    
    async def getmtime_ns(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.stat(
            dstpath,
            flags=flags,
            follow_symlinks=follow_symlinks,
        )

        mtime_ns: float | None = None
        if attrs.atime is not None:
            mtime_ns = tuple_to_nsec(attrs.mtime, attrs.mtime_ns)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            TransferResult(
                file_path=dstpath,
                file_type="STATS",
                file_attribues=FileAttributes(
                    mtime_ns=mtime_ns,
                ),
                file_transfer_elapsed=elapsed,
            )
        )

    async def getsize(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)
        
        start = time.monotonic()
        attrs = await self._handler.stat(
            dstpath,
            flags,
            follow_symlinks=follow_symlinks,
        )

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=FileAttributes(
                        size=attrs.size,
                    ),
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def isdir(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)
        
        start = time.monotonic()
        attrs = await self._handler.stat(
            dstpath,
            flags,
            follow_symlinks=follow_symlinks,
        )

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="DIRECTORY" if attrs.type == FILEXFER_TYPE_DIRECTORY else None,
                    file_attribues=TransferResult.to_attributes(attrs),
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def isfile(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)
        
        start = time.monotonic()
        attrs = await self._handler.stat(
            dstpath,
            flags,
            follow_symlinks=follow_symlinks,
        )

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="FILE" if attrs.type == FILEXFER_TYPE_REGULAR else None,
                    file_attribues=TransferResult.to_attributes(attrs),
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def islink(
        self,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        flags: int = FILEXFER_ATTR_DEFINED_V4,
    ) -> bool:
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)
        
        start = time.monotonic()
        attrs = await self._handler.stat(
            dstpath,
            flags,
            follow_symlinks=follow_symlinks,
        )

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="SYMLINK" if attrs.type == FILEXFER_TYPE_SYMLINK else None,
                    file_attribues=TransferResult.to_attributes(attrs),
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def remove(
        self,
        path: str | pathlib.PurePath,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()
        await self._handler.remove(dstpath)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_transfer_elapsed=elapsed
                )
            }
        )

    async def unlink(
        self,
        path: str | pathlib.PurePath,
    ):
        """Remove a remote file (see :meth:`remove`)"""

        return await self.remove(path)
    
    async def rename(
        self,
        oldpath: str | pathlib.PurePath,
        newpath: str | pathlib.PurePath,
        flags: int = 0,
    ):
        
        if isinstance(oldpath, pathlib.Path):
            oldpath = str(oldpath)

        if isinstance(newpath, pathlib.Path):
            newpath = str(newpath)

        srcpath: bytes = oldpath.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in srcpath:
            srcpath = posixpath.join(self._base_directory, srcpath)

        dstpath: bytes = newpath.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()
        await self._handler.rename(srcpath, dstpath, flags)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_data=srcpath,
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def posix_rename(
        self,
        oldpath: str | pathlib.PurePath,
        newpath: str | pathlib.PurePath,
    ):
        
        if isinstance(oldpath, pathlib.Path):
            oldpath = str(oldpath)

        if isinstance(newpath, pathlib.Path):
            newpath = str(newpath)

        srcpath: bytes = oldpath.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in srcpath:
            srcpath = posixpath.join(self._base_directory, srcpath)

        dstpath: bytes = newpath.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        await self._handler.posix_rename(oldpath, newpath)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_data=srcpath,
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def scandir(
        self,
        path: str | pathlib.PurePath = '.',
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dirpath: bytes = path.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dirpath:
            dirpath = posixpath.join(self._base_directory, dirpath)

        entries: dict[bytes, TransferResult] = {}
        
        start = time.monotonic()
        async for entry in self._scandir(dirpath):
            
            filename = entry.filename
            if isinstance(filename, str):
                filename = filename.encode(encoding=self._path_encoding)

            if entry.longname and isinstance(entry.longname, str):
                filename = entry.longname.encode(encoding=self._path_encoding)

            elif entry.longname:
                filename = entry.longname

            entries[filename] = TransferResult(
                file_path=filename,
                file_attribues=TransferResult.to_attributes(entry.attrs),
                file_transfer_elapsed=time.monotonic() - start
            )

        elapsed = time.monotonic() - start
        return (
            elapsed,
            entries
        )


    async def _scandir(
        self,
        dirpath: bytes,
    ):
        handle = await self._handler.opendir(dirpath)
        at_end = False


        while not at_end:
            names, at_end = await self._handler.readdir(handle)

            for entry in names:
                yield entry

    async def mkdir(
        self,
        path: str | pathlib.PurePath,
        attributes: FileAttributes,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()   
        await self._handler.mkdir(path, attributes)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_attribues=attributes,
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def rmdir(
        self,
        path: str | pathlib.PurePath,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()   
        await self._handler.rmdir(path)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def realpath(
        self,
        path: str | pathlib.PurePath,
        compose_paths: list[str | pathlib.PurePath] | None = None,
        check: int = FXRP_NO_CHECK,
    ):

        if compose_paths and isinstance(compose_paths[-1], int):
            check = compose_paths[-1]
            compose_paths = compose_paths[:-1]

        elif compose_paths is None:
            compose_paths = []

        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)


        start = time.monotonic()

        if self.version >= 6:
            compose_paths = [
                compose_path.encode() for compose_path in compose_paths
            ]

            names, _ = await self._handler.realpath(
                dstpath,
                *compose_paths,
                check=check,
            )

        else:

            composed_paths: list[bytes] = []

            for composed in compose_paths:
                if isinstance(composed, pathlib.Path):
                    composed = str(composed)

                composed_path_bytes: bytes = composed.encode(encoding=self._path_encoding)
                if self._base_directory and self._base_directory not in composed_path_bytes:
                    composed_path_bytes = posixpath.join(self._base_directory, composed_path_bytes)

                composed_paths.append(composed_path_bytes)

            for composed_path in composed_paths:
                dstpath = posixpath.join(composed_path, dstpath)

            names, _ = await self._handler.realpath(dstpath)

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
                    
                    names[0].attrs = FileAttributes(type=FILEXFER_TYPE_UNKNOWN)

            selected_sftp_name = names[0]

            filepath = selected_sftp_name.filename
            if isinstance(filepath, str):
                filepath = filepath.encode(encoding=self._path_encoding)

            if selected_sftp_name.longname and isinstance(selected_sftp_name.longname, str):
                filepath = selected_sftp_name.longname.encode(encoding=self._path_encoding)

            elif selected_sftp_name.longname:
                filepath = selected_sftp_name.longname

            result = TransferResult(
                file_path=filepath,
                file_attribues=TransferResult.to_attributes(selected_sftp_name.attrs),
                file_transfer_elapsed=time.monotonic() - start,
            )
        
        else:

            selected_sftp_name = names[0]
            filepath = selected_sftp_name.filename

            if isinstance(filepath, str):
                filepath = filepath.encode(encoding=self._path_encoding)
            
            result = TransferResult(
                file_path=filepath,
                file_attribues=TransferResult.to_attributes(selected_sftp_name.attrs),
                file_transfer_elapsed=time.monotonic() - start,
            )

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: result,
            },
        )
        
    def _create_compose_paths(
        self,
        compose_paths: list[str | pathlib.Path],
    ) -> list[bytes]:

        composed_paths: list[bytes] = []

        for composed in compose_paths:
            if isinstance(composed, pathlib.Path):
                composed = str(composed)

            composed_path_bytes: bytes = composed.encode(encoding=self._path_encoding)
            if self._base_directory and self._base_directory not in composed_path_bytes:
                composed_path_bytes = posixpath.join(self._base_directory, composed_path_bytes)

            composed_paths.append(composed_path_bytes)

        return composed_paths


    async def getcwd(self):
        (elapsed, result) = await self.realpath('.')
        return (
            elapsed,
            result,
        )

    async def chdir(
        self,
        path: str | pathlib.PurePath,
    ):
        (elapsed, result) = await self.realpath(path)

        for res in result.values():
            self._base_directory = res.file_path

        return (
            elapsed,
            result,
        )

    async def readlink(
        self,
        path: str | pathlib.PurePath,
    ):

        if isinstance(path, pathlib.Path):
            path = str(path)

        linkpath: bytes = path.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in linkpath:
            linkpath = posixpath.join(self._base_directory, linkpath)

        start = time.monotonic()
        names, _ = await self._handler.readlink(linkpath)

        if len(names) > 1:
            raise SFTPBadMessage('Too many names returned')

        selected_sftp_name = names[0]
        filepath = selected_sftp_name.filename

        if isinstance(filepath, str):
            filepath = filepath.encode(encoding=self._path_encoding)
        
        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                linkpath: TransferResult(
                    file_path=filepath,
                    file_attribues=TransferResult.to_attributes(selected_sftp_name.attrs),
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def symlink(
        self,
        oldpath: str | pathlib.PurePath,
        newpath: str | pathlib.PurePath,
    ) -> None:

        if isinstance(oldpath, pathlib.PurePath):
            oldpath = str(oldpath)

        if isinstance(newpath, pathlib.Path):
            newpath = str(newpath)

        srcpath = oldpath.encode(encoding=self._path_encoding)

        dstpath: bytes = newpath.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()
        await self._handler.symlink(srcpath, dstpath)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_data=srcpath,
                    file_type="SYMLINK",
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def link(
        self,
        oldpath: str | pathlib.PurePath,
        newpath: str | pathlib.PurePath,
    ) -> None:

        if isinstance(oldpath, pathlib.PurePath):
            oldpath = str(oldpath)

        if isinstance(newpath, pathlib.Path):
            newpath = str(newpath)

        srcpath: bytes = oldpath.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in srcpath:
            srcpath = posixpath.join(self._base_directory, srcpath)

        dstpath: bytes = newpath.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()
        await self._handler.link(srcpath, dstpath)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_data=srcpath,
                    file_type="HARDLINK",
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    def exit(self) -> None:
        self._handler.exit()

    async def wait_closed(self) -> None:
        await self._handler.wait_closed()
