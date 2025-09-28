import asyncio
import pathlib
import posixpath
import time
from collections import deque
from typing import Deque

from hyperscale.core.engines.client.ssh.protocol.ssh.constants import (
    ACE4_APPEND_DATA,
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
from hyperscale.core.testing.models import (
    File,
    FileGlob,
)
from hyperscale.core.testing.models.file.file_attributes import FileAttributes

from .models import (
    SFTPOptions,
    TransferResult,
)
from .protocols.sftp import (
    SFTPGlob,
    SFTPBadMessage,
    SFTPPermissionDenied,
    SFTPFileAlreadyExists,
    SFTPFailure,
    SFTPClientFile,
    SFTPError,
    SFTPNotADirectory,
    SFTPLimits,
    SFTPAttrs,
    mode_to_pflags,
    utime_to_attrs,
    tuple_to_float_sec,
    tuple_to_nsec,
    valid_attr_flags,
    SFTPClientHandler,
)



class SFTPCommand:

    def __init__(
        self,
        handler: SFTPClientHandler,
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

    async def get(
        self,
        path: str | pathlib.PurePath,
        options: SFTPOptions
    ):
        
        transferred: dict[bytes, TransferResult] = {}
        
        operation_start = time.monotonic()
        remote_path = await self._stat_remote_paths(
            path,
            flags=options.flags,
        )

        found: Deque[tuple[bytes, FileAttributes]] = deque([remote_path])

        while len(found):
            (
                srcpath,
                attributes,
            ) = found.pop()

            start = time.monotonic()

            filetype = attributes.type

            if options.follow_symlinks and filetype == FILEXFER_TYPE_SYMLINK:

                srcattrs = await self._handler.stat(
                    srcpath,
                    options.flags,
                    follow_symlinks=options.follow_symlinks,
                )

                attributes = srcattrs.to_file_attributes()
                filetype = srcattrs.type

            if filetype == FILEXFER_TYPE_DIRECTORY and options.recurse:

                async for srcname in self._scandir(srcpath):
                    filename: bytes = srcname.filename

                    if filename in (b'.', b'..'):
                        continue

                    srcfile = posixpath.join(srcpath, filename)

                    found.append((
                        srcfile,
                        srcname.attrs.to_file_attributes(),
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

                if self._handler.version < 5:
                    handle = await self._handler.open(
                        srcpath, 
                        options.flags,
                        attributes,
                    )

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

                    handle = await self._handler.open56(
                        srcpath, 
                        options.desired_access, 
                        options.flags, 
                        attributes,
                    )

                    src = SFTPClientFile(
                        srcpath,
                        self._handler,
                        handle,
                        bool(
                            options.desired_access 
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
                    file_path=srcpath,
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
        attributes: FileAttributes | None,
        data: bytes | str | File,
        options: SFTPOptions,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if isinstance(data, str):
            data: bytes = await data.encode()

        elif isinstance(data, File):
            (
                dstpath,
                data,
                attributes,
            ) = await data.optimized

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        if attributes is None:
            attributes = self._create_default_attributes(encoded_data=data)

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
                options.flags,
                follow_symlinks=options.follow_symlinks,
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

            if self._handler.version < 5:
                handle = await self._handler.open(
                    dstpath, 
                    options.flags,
                    attributes,
                )

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
                handle = await self._handler.open56(
                    dstpath, 
                    options.desired_access,
                    options.flags, 
                    attributes,
                )

                dst = SFTPClientFile(
                    dstpath,
                    self._handler,
                    handle,
                    bool(
                        options.desired_access 
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
        options: SFTPOptions,
    ):

        transferred: dict[bytes, TransferResult] = {}
        operation_start = time.monotonic()
        remote_path = await self._stat_remote_paths(
            path,
            flags=options.flags,
        )

        found: Deque[tuple[bytes, FileAttributes]] = deque([remote_path])

        while len(found):
            (
                srcpath,
                attributes,
            ) = found.pop()

            start = time.monotonic()

            filetype = attributes.type

            if options.follow_symlinks and filetype == FILEXFER_TYPE_SYMLINK:
                srcattrs = await self._handler.stat(
                    srcpath,
                    options.flags,
                    follow_symlinks=options.follow_symlinks,
                )

                attributes = srcattrs.to_file_attributes()
                filetype = attributes.type

            if filetype == FILEXFER_TYPE_DIRECTORY and options.recurse:

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
                        srcname.attrs.to_file_attributes(),
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

                if self._handler.version < 5:
                    handle = await self._handler.open(
                        srcpath,
                        options.flags,
                        attributes,
                    )

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

                    handle = await self._handler.open56(
                        srcpath, 
                        options.desired_access,
                        options.flags, 
                        attributes,
                    )

                    src = SFTPClientFile(
                        srcpath,
                        self._handler,
                        handle,
                        bool(
                            options.desired_access 
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

                if self._handler.version < 5:
                    handle = await self._handler.open(
                        srcpath,
                        options.flags,
                        attributes,
                    )

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
                    handle = await self._handler.open56(
                        srcpath, 
                        options.desired_access, 
                        options.flags,
                        attributes,
                    )
                    
                    dst = SFTPClientFile(
                        srcpath,
                        copy_handler,
                        handle,
                        bool(
                            options.desired_access 
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
                    file_path=srcpath,
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
        pattern: str,
        options: SFTPOptions,
    ):

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

            if options.follow_symlinks and filetype == FILEXFER_TYPE_SYMLINK:

                srcattrs = await self._handler.stat(
                    srcpath,
                    options.flags,
                    follow_symlinks=options.follow_symlinks,
                )

                attributes = srcattrs.to_file_attributes()
                filetype = srcattrs.type

            if filetype == FILEXFER_TYPE_DIRECTORY and options.recurse:

                async for srcname in self._scandir(srcpath):
                    filename: bytes = srcname.filename

                    if filename in (b'.', b'..'):
                        continue

                    srcfile = posixpath.join(srcpath, filename)

                    found.append((
                        srcfile,
                        srcname.attrs.to_file_attributes(),
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

                if self._handler.version < 5:
                    handle = await self._handler.open(srcpath, options.flags, attributes)
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

                    handle = await self._handler.open56(
                        srcpath,
                        options.desired_access,
                        options.flags,
                        attributes,
                    )

                    src = SFTPClientFile(
                        srcpath,
                        self._handler,
                        handle,
                        bool(
                            options.desired_access 
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
                    file_path=srcpath,
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
        path: str,
        attributes: FileAttributes | None,
        data: bytes | str | FileGlob,
        options: SFTPOptions,
     ):
        transferred: dict[bytes, TransferResult] = {}

        if isinstance(data, FileGlob):
            found = await data.optimized

        else:

            if isinstance(path, pathlib.Path):
                path = str(path)

            dstpath: bytes = path.encode(encoding=self._path_encoding)

            if isinstance(data, str):
                data = data.encode()

            if self._base_directory and self._base_directory not in dstpath:
                dstpath = posixpath.join(self._base_directory, dstpath)

            if attributes is None:
                attributes = self._create_default_attributes(encoded_data=data)

            found = [(
                dstpath,
                data,
                attributes,
            )]
            
        operation_start = time.monotonic()

        for dstpath, data, attrs in found:

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
                    options.flags,
                    follow_symlinks=options.follow_symlinks,
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


                if self._handler.version < 5:
                    handle = await self._handler.open(dstpath, options.flags, attrs)
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
                    handle = await self._handler.open56(
                        dstpath,
                        options.desired_access,
                        options.flags,
                        attrs,
                    )

                    dst = SFTPClientFile(
                        dstpath,
                        self._handler,
                        handle,
                        bool(
                            options.desired_access 
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
        pattern: str,
        options: SFTPOptions,
    ):
      
        transferred: dict[bytes, TransferResult] = {}
        glob = SFTPGlob(self._handler, False)
        found = await self._glob_remote_paths(
            pattern,
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

            if options.follow_symlinks and filetype == FILEXFER_TYPE_SYMLINK:
                srcattrs = await self._handler.stat(
                    srcpath,
                    options.flags,
                    follow_symlinks=options.follow_symlinks,
                )

                attributes = srcattrs.to_file_attributes()
                filetype = attributes.type

            if filetype == FILEXFER_TYPE_DIRECTORY and options.recurse:

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
                        srcname.attrs.to_file_attributes(),
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

                if self._handler.version < 5:
                    handle = await self._handler.open(srcpath, options.flags, attributes)
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

                    handle = await self._handler.open56(
                        srcpath,
                        options.desired_access,
                        options.flags,
                        attributes,
                    )

                    src = SFTPClientFile(
                        srcpath,
                        self._handler,
                        handle,
                        bool(
                            options.desired_access 
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

                if self._handler.version < 5:
                    handle = await self._handler.open(srcpath, options.flags, attributes)
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
                    handle = await self._handler.open56(
                        srcpath,
                        options.desired_access,
                        options.flags,
                        attributes,
                    )

                    dst = SFTPClientFile(
                        srcpath,
                        copy_handler,
                        handle,
                        bool(
                            options.desired_access 
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
                    file_path=srcpath,
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
        transferred: dict[bytes, TransferResult] = {
                srcpath: TransferResult(
                    file_path=srcpath,
                    file_attribues=attrs,
                    file_type=TransferResult.to_file_type(attrs.type),
                    file_transfer_elapsed=elapsed,
                ) for srcpath, attrs in discovered
            }

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
        transferred: dict[bytes, TransferResult] = {
            srcpath: TransferResult(
                file_path=srcpath,
                file_attribues=attrs,
                file_type=TransferResult.to_file_type(attrs.type),
                file_transfer_elapsed=elapsed,
            )  for srcpath, attrs in discovered
        }

        return (
            elapsed,
            transferred,
        )

    async def makedirs(
        self,
        path: str | pathlib.PurePath,
        attributes: FileAttributes | None,
        options: SFTPOptions,
    ):

        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        transferred: dict[bytes, TransferResult] = {}
        operation_start = time.monotonic() - start

        if attributes is None:
            attributes = self._create_default_attributes()

        start = time.monotonic()

        await self._make_directories(
            dstpath,
            attributes,
            exist_ok=options.exist_ok,
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

        found: Deque[tuple[bytes, FileAttributes]] = deque([
            (path, attrs)
            for path, attrs in discovered if attrs.type != FILEXFER_TYPE_SYMLINK
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
                        entry.attrs.to_file_attributes(),
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

        attributes = (
            await self._handler.stat(
                srcpath,
                flags,
                follow_symlinks=True,
            )
        ).to_file_attributes()

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
            sftp_version=self._handler.version,
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
                match.attrs.to_file_attributes()
            ))

        return srcpaths

    async def _make_directories(
        self,
        path: bytes,
        attrs: FileAttributes,
        options: SFTPOptions,
    ):
        curpath = b'/' if posixpath.isabs(path) else b''
        parts = path.split(b'/')
        last = len(parts) - 1

        exists = True

        for i, part in enumerate(parts):
            curpath = posixpath.join(curpath, part)

            try:
                await self._handler.mkdir(path, attrs)
                exists = False

            except (SFTPFailure, SFTPFileAlreadyExists):
                filetype = await self._handler.stat(
                    curpath,
                    options.flags,
                    follow_symlinks=options.follow_symlinks,
                )

                if filetype != FILEXFER_TYPE_DIRECTORY:
                    curpath_str = curpath.decode('utf-8', 'backslashreplace')

                    exc = SFTPNotADirectory if self._handler.version >= 6 \
                        else SFTPFailure

                    raise exc(f'{curpath_str} is not a directory') from None
            except SFTPPermissionDenied:
                if i == last:
                    raise

            if exists and not options.exist_ok:
                exc = SFTPFileAlreadyExists if self._handler.version >= 6 else SFTPFailure

                raise exc(curpath.decode('utf-8', 'backslashreplace') +
                        ' already exists')

        return path

    async def stat(
        self,
        path: str | pathlib.PurePath,
        options: SFTPOptions,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.stat(
            dstpath,
            options.flags,
            follow_symlinks=options.follow_symlinks,
        )

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

    async def lstat(
        self,
        path: str | pathlib.PurePath,
        options: SFTPOptions,
    ):

        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.lstat(dstpath, options.flags)

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
        attributes: FileAttributes | None,
        options: SFTPOptions,
    ):

        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        if attributes is None:
            attributes = self._create_default_attributes()

        start = time.monotonic()

        await self._handler.setstat(
            path,
            attributes,
            follow_symlinks=options.follow_symlinks,
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
    ):

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
        options: SFTPOptions,
    ):
        return await self.setstat(
            path,
            FileAttributes(size=size),
            options,
        )

    async def chown(
        self, 
        path: str | pathlib.PurePath,
        options: SFTPOptions,
    ):
        return await self.setstat(
            path,
            FileAttributes(
                uid=options.uid,
                gid=options.gid,
                owner=options.owner,
                group=options.group,
            ),
            options,
        )

    async def chmod(
        self,
        path: str | pathlib.PurePath,
        options: SFTPOptions,
    ):
        return await self.setstat(
            path,
            FileAttributes(permissions=options.permissions),
            options,
        )

    async def utime(
        self,
        path: str | pathlib.PurePath,
        options: SFTPOptions,
    ):
        return await self.setstat(
            path,
            TransferResult.to_attributes(
                utime_to_attrs(options.times, options.nanoseconds),
            ),
            options,
        )

    async def exists(
        self,
        path: str | pathlib.PurePath,
        options: SFTPOptions,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()
        
        attrs = await self._handler.stat(
            dstpath,
            flags=options.flags,
            follow_symlinks=options.follow_symlinks,
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
        options: SFTPOptions,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()
        
        attrs = await self._handler.lstat(
            dstpath,
            flags=options.flags,
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
        options: SFTPOptions,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.stat(
            dstpath,
            flags=options.flags,
            follow_symlinks=options.follow_symlinks,
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
        options: SFTPOptions,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        attrs = await self._handler.stat(
            dstpath,
            flags=options.flags,
            follow_symlinks=options.follow_symlinks,
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
        options: SFTPOptions,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.stat(
            dstpath,
            flags=options.flags,
            follow_symlinks=options.follow_symlinks,
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
        options: SFTPOptions,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        attrs = await self._handler.stat(
            dstpath,
            flags=options.flags,
            follow_symlinks=options.follow_symlinks,
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
        options: SFTPOptions,
    ):
        
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.stat(
            dstpath,
            flags=options.flags,
            follow_symlinks=options.follow_symlinks,
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
        options: SFTPOptions,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        start = time.monotonic()

        attrs = await self._handler.stat(
            dstpath,
            flags=options.flags,
            follow_symlinks=options.follow_symlinks,
        )

        mtime_ns: float | None = None
        if attrs.atime is not None:
            mtime_ns = tuple_to_nsec(attrs.mtime, attrs.mtime_ns)

        elapsed = time.monotonic() - start
        return (
            elapsed,
            {
                dstpath: TransferResult(
                    file_path=dstpath,
                    file_type="STATS",
                    file_attribues=FileAttributes(
                        mtime_ns=mtime_ns,
                    ),
                    file_transfer_elapsed=elapsed,
                )
            }
        )

    async def getsize(
        self,
        path: str | pathlib.PurePath,
        options: SFTPOptions,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)
        
        start = time.monotonic()
        attrs = await self._handler.stat(
            dstpath,
            options.flags,
            follow_symlinks=options.follow_symlinks,
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
        options: SFTPOptions,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)
        
        start = time.monotonic()
        attrs = await self._handler.stat(
            dstpath,
            options.flags,
            follow_symlinks=options.follow_symlinks,
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
        options: SFTPOptions,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)
        
        start = time.monotonic()
        attrs = await self._handler.stat(
            dstpath,
            options.flags,
            follow_symlinks=options.follow_symlinks,
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
        options: SFTPOptions,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)

        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)
        
        start = time.monotonic()
        attrs = await self._handler.stat(
            dstpath,
            options.flags,
            follow_symlinks=options.follow_symlinks,
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
        options: SFTPOptions,
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
        await self._handler.rename(srcpath, dstpath, options.flags)

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
        attributes: FileAttributes | None,
    ):
        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)

        if attributes is None:
            attributes = self._create_default_attributes()

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
        options: SFTPOptions,
    ):

        compose_paths = options.compose_paths
        check = options.check

        if compose_paths and isinstance(compose_paths[-1], int):
            check = compose_paths[-1]
            compose_paths = compose_paths[:-1]

        if isinstance(path, pathlib.Path):
            path = str(path)

        dstpath: bytes = path.encode(encoding=self._path_encoding)
        if self._base_directory and self._base_directory not in dstpath:
            dstpath = posixpath.join(self._base_directory, dstpath)


        start = time.monotonic()

        if self._handler.version >= 6:
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
            if self._handler.version < 6:

                filename = names[0].filename
                if isinstance(filename, str):
                    filename = filename.encode(encoding=self._path_encoding)

                try:
                    names[0].attrs = await self._handler.stat(
                        filename,
                        valid_attr_flags[self._handler.version],
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


    async def getcwd(
        self,
        options: SFTPOptions,
    ):
        (elapsed, result) = await self.realpath('.', options)
        return (
            elapsed,
            result,
        )

    async def chdir(
        self,
        path: str | pathlib.PurePath,
        options: SFTPOptions,
    ):
        (elapsed, result) = await self.realpath(path, options)

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
    ):

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
    ):

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
    
    def _create_default_attributes(
        self,
        encoded_data: bytes | None = None,
    ):

        created = time.monotonic()
        created_ns = time.monotonic_ns()

        return FileAttributes(
            type=TransferResult.to_file_type_int("FILE"),
            size=len(encoded_data) if encoded_data else 0,
            uid=1000,
            gid=1000,
            permissions=644,
            crtime=created,
            crtime_ns=created_ns,
            atime=created,
            atime_ns=created_ns,
            ctime=created,
            ctime_ns=created_ns,
            created=created,
            mtime_ns=created_ns,
            mime_type="application/octet-stream",
        )

    def exit(self) -> None:
        self._handler.exit()

    async def wait_closed(self) -> None:
        await self._handler.wait_closed()
