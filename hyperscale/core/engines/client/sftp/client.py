import asyncio
import posixpath
from pathlib import PurePath
from typing import (
    Optional, 
    Type, 
    Sequence, 
    AsyncIterator,
    overload,
    cast,
)
from .record import Record


class SFTPLimits(Record):
    """SFTP server limits

       SFTPLimits is a simple record class with the following fields:

         ================= ========================================= ======
         Field             Description                               Type
         ================= ========================================= ======
         max_packet_len    Max allowed size of an SFTP packet        uint64
         max_read_len      Max allowed size of an SFTP read request  uint64
         max_write_len     Max allowed size of an SFTP write request uint64
         max_open_handles  Max allowed number of open file handles   uint64
         ================= ========================================= ======

    """

    max_packet_len: int
    max_read_len: int
    max_write_len: int
    max_open_handles: int

    def encode(self) -> bytes:
        """Encode SFTP server limits in an SSH packet"""

        return (UInt64(self.max_packet_len) + UInt64(self.max_read_len) +
                UInt64(self.max_write_len) + UInt64(self.max_open_handles))
    


class SFTPName(Record):
    """SFTP file name and attributes

       SFTPName is a simple record class with the following fields:

         ========= ================================== ==================
         Field     Description                        Type
         ========= ================================== ==================
         filename  Filename                           `str` or `bytes`
         longname  Expanded form of filename & attrs  `str` or `bytes`
         attrs     File attributes                    :class:`SFTPAttrs`
         ========= ================================== ==================

       A list of these is returned by :meth:`readdir() <SFTPClient.readdir>`
       in :class:`SFTPClient` when retrieving the contents of a directory.

    """

    filename: bytes | str = ''
    longname: bytes | str = ''
    attrs: SFTPAttrs = SFTPAttrs()

    def _format(self, k: str, v: object) -> Optional[str]:
        """Convert name fields to more readable values"""

        if k == 'longname' and not v:
            return None

        if isinstance(v, bytes):
            v = v.decode('utf-8', 'backslashreplace')

        return str(v) or None

class SFTPClient:
    """SFTP client

       This class represents the client side of an SFTP session. It is
       started by calling the :meth:`start_sftp_client()
       <SSHClientConnection.start_sftp_client>` method on the
       :class:`SSHClientConnection` class.

    """

    def __init__(self, handler: SFTPClientHandler,
                 path_encoding: Optional[str], path_errors: str):
        self._handler = handler
        self._path_encoding = path_encoding
        self._path_errors = path_errors
        self._cwd: Optional[bytes] = None

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

    def encode(self, path: bytes | str | PurePath) -> bytes:
        """Encode path name using configured path encoding

           This method has no effect if the path is already bytes.

        """

        if isinstance(path, PurePath):
            path = str(path)

        if isinstance(path, str):
            if self._path_encoding:
                path = path.encode(self._path_encoding, self._path_errors)
            else:
                raise Exception('Path must be bytes when '
                                     'encoding is not set')

        return path

    def decode(self, path: bytes, want_string: bool = True) -> str | bytes:
        """Decode path name using configured path encoding

           This method has no effect if want_string is set to `False`.

        """

        if want_string and self._path_encoding:
            try:
                return path.decode(self._path_encoding, self._path_errors)
            except UnicodeDecodeError:
                raise Exception('Unable to decode name') from None

        return path

    def compose_path(self, path: bytes | str | PurePath,
                     parent: Optional[bytes] = None) -> bytes:
        """Compose a path

           If parent is not specified, return a path relative to the
           current remote working directory.

        """

        if parent is None:
            parent = self._cwd

        path = self.encode(path)

        return posixpath.join(parent, path) if parent else path

    async def _type(self, path: bytes | str | PurePath,
                    statfunc: Optional[_SFTPStatFunc] = None) -> int:
        """Return the file type of a remote path, or FILEXFER_TYPE_UNKNOWN
           if it can't be accessed"""

        if statfunc is None:
            statfunc = self.stat

        try:
            return (await statfunc(path)).type
        except (SFTPNoSuchFile, SFTPNoSuchPath, SFTPPermissionDenied):
            return FILEXFER_TYPE_UNKNOWN

    async def _copy(self, srcfs: _SFTPFSProtocol, dstfs: _SFTPFSProtocol,
                    srcpath: bytes, dstpath: bytes, srcattrs: SFTPAttrs,
                    preserve: bool, recurse: bool, follow_symlinks: bool,
                    sparse: bool, block_size: int, max_requests: int,
                    progress_handler: SFTPProgressHandler,
                    error_handler: ExceptionHandler,
                    remote_only: bool) -> None:
        """Copy a file, directory, or symbolic link"""

        try:
            filetype = srcattrs.type

            if follow_symlinks and filetype == FILEXFER_TYPE_SYMLINK:
                srcattrs = await srcfs.stat(srcpath)
                filetype = srcattrs.type

            if filetype == FILEXFER_TYPE_DIRECTORY:
                if not recurse:
                    exc = SFTPFileIsADirectory if self.version >= 6 \
                        else SFTPFailure

                    raise exc(srcpath.decode('utf-8', 'backslashreplace') +
                              ' is a directory')

                self.logger.info('  Starting copy of directory %s to %s',
                                 srcpath, dstpath)

                if not await dstfs.isdir(dstpath):
                    await dstfs.mkdir(dstpath)

                async for srcname in srcfs.scandir(srcpath):
                    filename = cast(bytes, srcname.filename)

                    if filename in (b'.', b'..'):
                        continue

                    srcfile = posixpath.join(srcpath, filename)
                    dstfile = posixpath.join(dstpath, filename)

                    await self._copy(srcfs, dstfs, srcfile, dstfile,
                                     srcname.attrs, preserve, recurse,
                                     follow_symlinks, sparse, block_size,
                                     max_requests, progress_handler,
                                     error_handler, remote_only)

                self.logger.info('  Finished copy of directory %s to %s',
                                 srcpath, dstpath)

            elif filetype == FILEXFER_TYPE_SYMLINK:
                targetpath = await srcfs.readlink(srcpath)

                self.logger.info('  Copying symlink %s to %s', srcpath, dstpath)
                self.logger.info('    Target path: %s', targetpath)

                await dstfs.symlink(targetpath, dstpath)
            else:
                self.logger.info('  Copying file %s to %s', srcpath, dstpath)

                if remote_only and not self.supports_remote_copy:
                    raise SFTPOpUnsupported('Remote copy not supported')

                await _SFTPFileCopier(block_size, max_requests,
                                      srcattrs.size or 0, sparse,
                                      srcfs, dstfs, srcpath, dstpath,
                                      progress_handler).run()

            if preserve:
                attrs = await srcfs.stat(srcpath,
                                         follow_symlinks=follow_symlinks)

                attrs = SFTPAttrs(permissions=attrs.permissions,
                                  atime=attrs.atime, atime_ns=attrs.atime_ns,
                                  mtime=attrs.mtime, mtime_ns=attrs.mtime_ns)

                try:
                    await dstfs.setstat(dstpath, attrs,
                                        follow_symlinks=follow_symlinks or
                                        filetype != FILEXFER_TYPE_SYMLINK)

                    self.logger.info('    Preserved attrs: %s', attrs)
                except SFTPOpUnsupported:
                    self.logger.info('    Preserving symlink attrs unsupported')

        except (OSError, Exception) as exc:
            setattr(exc, 'srcpath', srcpath)
            setattr(exc, 'dstpath', dstpath)

            if error_handler:
                error_handler(exc)
            else:
                raise

    async def _begin_copy(self, srcfs: _SFTPFSProtocol, dstfs: _SFTPFSProtocol,
                          srcpaths: bytes | str | PurePath, dstpath: Optional[bytes | str | PurePath],
                          copy_type: str, expand_glob: bool, preserve: bool,
                          recurse: bool, follow_symlinks: bool, sparse: bool,
                          block_size: int, max_requests: int,
                          progress_handler: SFTPProgressHandler,
                          error_handler: ExceptionHandler,
                          remote_only: bool = False) -> None:
        """Begin a new file upload, download, or copy"""

        if block_size <= 0:
            block_size = min(srcfs.limits.max_read_len,
                             dstfs.limits.max_write_len)

        if max_requests <= 0:
            max_requests = max(16, min(MAX_SFTP_READ_LEN // block_size, 128))

        if isinstance(srcpaths, (bytes, str, PurePath)):
            srcpaths = [srcpaths]
        elif not isinstance(srcpaths, list):
            srcpaths = list(srcpaths)

        self.logger.info('Starting SFTP %s of %s to %s',
                         copy_type, srcpaths, dstpath)

        srcnames: List[SFTPName] = []

        if expand_glob:
            glob = SFTPGlob(srcfs, len(srcpaths) > 1)

            for srcpath in srcpaths:
                srcnames.extend(await glob.match(srcfs.encode(srcpath),
                                                 error_handler, self.version))
        else:
            for srcpath in srcpaths:
                srcpath = srcfs.encode(srcpath)
                srcattrs = await srcfs.stat(srcpath,
                                            follow_symlinks=follow_symlinks)
                srcnames.append(SFTPName(srcpath, attrs=srcattrs))

        if dstpath:
            dstpath = dstfs.encode(dstpath)

        dstpath: Optional[bytes]

        dst_isdir = dstpath is None or (await dstfs.isdir(dstpath))

        if len(srcnames) > 1 and not dst_isdir:
            assert dstpath is not None
            exc = SFTPNotADirectory if self.version >= 6 else SFTPFailure

            raise exc(dstpath.decode('utf-8', 'backslashreplace') +
                      ' must be a directory')

        for srcname in srcnames:
            srcfile = cast(bytes, srcname.filename)
            basename = srcfs.basename(srcfile)

            if dstpath is None:
                dstfile = basename
            elif dst_isdir:
                dstfile = dstfs.compose_path(basename, parent=dstpath)
            else:
                dstfile = dstpath

            await self._copy(srcfs, dstfs, srcfile, dstfile, srcname.attrs,
                             preserve, recurse, follow_symlinks, sparse,
                             block_size, max_requests, progress_handler,
                             error_handler, remote_only)

    async def get(self, remotepaths: bytes | str | PurePath,
                  localpath: Optional[bytes | str | PurePath] = None, *,
                  preserve: bool = False, recurse: bool = False,
                  follow_symlinks: bool = False, sparse: bool = True,
                  block_size: int = -1, max_requests: int = -1,
                  progress_handler: SFTPProgressHandler = None,
                  error_handler: ExceptionHandler = None) -> None:
        """Download remote files

           This method downloads one or more files or directories from
           the remote system. Either a single remote path or a sequence
           of remote paths to download can be provided.

           When downloading a single file or directory, the local path can
           be either the full path to download data into or the path to an
           existing directory where the data should be placed. In the
           latter case, the base file name from the remote path will be
           used as the local name.

           When downloading multiple files, the local path must refer to
           an existing directory.

           If no local path is provided, the file is downloaded
           into the current local working directory.

           If preserve is `True`, the access and modification times
           and permissions of the original file are set on the
           downloaded file.

           If recurse is `True` and the remote path points at a
           directory, the entire subtree under that directory is
           downloaded.

           If follow_symlinks is set to `True`, symbolic links found
           on the remote system will have the contents of their target
           downloaded rather than creating a local symbolic link. When
           using this option during a recursive download, one needs to
           watch out for links that result in loops.

           The block_size argument specifies the size of read and write
           requests issued when downloading the files, defaulting to
           the maximum allowed by the server, or 16 KB if the server
           doesn't advertise limits.

           The max_requests argument specifies the maximum number of
           parallel read or write requests issued, defaulting to a
           value between 16 and 128 depending on the selected block
           size to avoid excessive memory usage.

           If progress_handler is specified, it will be called after
           each block of a file is successfully downloaded. The arguments
           passed to this handler will be the source path, destination
           path, bytes downloaded so far, and total bytes in the file
           being downloaded. If multiple source paths are provided or
           recurse is set to `True`, the progress_handler will be
           called consecutively on each file being downloaded.

           If error_handler is specified and an error occurs during
           the download, this handler will be called with the exception
           instead of it being raised. This is intended to primarily be
           used when multiple remote paths are provided or when recurse
           is set to `True`, to allow error information to be collected
           without aborting the download of the remaining files. The
           error handler can raise an exception if it wants the download
           to completely stop. Otherwise, after an error, the download
           will continue starting with the next file.

           :param remotepaths:
               The paths of the remote files or directories to download
           :param localpath: (optional)
               The path of the local file or directory to download into
           :param preserve: (optional)
               Whether or not to preserve the original file attributes
           :param recurse: (optional)
               Whether or not to recursively copy directories
           :param follow_symlinks: (optional)
               Whether or not to follow symbolic links
           :param sparse: (optional)
               Whether or not to do a sparse file copy where it is supported
           :param block_size: (optional)
               The block size to use for file reads and writes
           :param max_requests: (optional)
               The maximum number of parallel read or write requests
           :param progress_handler: (optional)
               The function to call to report download progress
           :param error_handler: (optional)
               The function to call when an error occurs
           :type remotepaths:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`,
               or a sequence of these
           :type localpath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type preserve: `bool`
           :type recurse: `bool`
           :type follow_symlinks: `bool`
           :type sparse: `bool`
           :type block_size: `int`
           :type max_requests: `int`
           :type progress_handler: `callable`
           :type error_handler: `callable`

           :raises: | :exc:`OSError` if a local file I/O error occurs
                    | :exc:`Exception` if the server returns an error

        """

        await self._begin_copy(self, local_fs, remotepaths, localpath, 'get',
                               False, preserve, recurse, follow_symlinks,
                               sparse, block_size, max_requests,
                               progress_handler, error_handler)

    async def put(self, localpaths: bytes | str | PurePath,
                  remotepath: Optional[bytes | str | PurePath] = None, *,
                  preserve: bool = False, recurse: bool = False,
                  follow_symlinks: bool = False, sparse: bool = True,
                  block_size: int = -1, max_requests: int = -1,
                  progress_handler: SFTPProgressHandler = None,
                  error_handler: ExceptionHandler = None) -> None:
        """Upload local files

           This method uploads one or more files or directories to the
           remote system. Either a single local path or a sequence of
           local paths to upload can be provided.

           When uploading a single file or directory, the remote path can
           be either the full path to upload data into or the path to an
           existing directory where the data should be placed. In the
           latter case, the base file name from the local path will be
           used as the remote name.

           When uploading multiple files, the remote path must refer to
           an existing directory.

           If no remote path is provided, the file is uploaded into the
           current remote working directory.

           If preserve is `True`, the access and modification times
           and permissions of the original file are set on the
           uploaded file.

           If recurse is `True` and the local path points at a
           directory, the entire subtree under that directory is
           uploaded.

           If follow_symlinks is set to `True`, symbolic links found
           on the local system will have the contents of their target
           uploaded rather than creating a remote symbolic link. When
           using this option during a recursive upload, one needs to
           watch out for links that result in loops.

           The block_size argument specifies the size of read and write
           requests issued when uploading the files, defaulting to
           the maximum allowed by the server, or 16 KB if the server
           doesn't advertise limits.

           The max_requests argument specifies the maximum number of
           parallel read or write requests issued, defaulting to a
           value between 16 and 128 depending on the selected block
           size to avoid excessive memory usage.

           If progress_handler is specified, it will be called after
           each block of a file is successfully uploaded. The arguments
           passed to this handler will be the source path, destination
           path, bytes uploaded so far, and total bytes in the file
           being uploaded. If multiple source paths are provided or
           recurse is set to `True`, the progress_handler will be
           called consecutively on each file being uploaded.

           If error_handler is specified and an error occurs during
           the upload, this handler will be called with the exception
           instead of it being raised. This is intended to primarily be
           used when multiple local paths are provided or when recurse
           is set to `True`, to allow error information to be collected
           without aborting the upload of the remaining files. The
           error handler can raise an exception if it wants the upload
           to completely stop. Otherwise, after an error, the upload
           will continue starting with the next file.

           :param localpaths:
               The paths of the local files or directories to upload
           :param remotepath: (optional)
               The path of the remote file or directory to upload into
           :param preserve: (optional)
               Whether or not to preserve the original file attributes
           :param recurse: (optional)
               Whether or not to recursively copy directories
           :param follow_symlinks: (optional)
               Whether or not to follow symbolic links
           :param sparse: (optional)
               Whether or not to do a sparse file copy where it is supported
           :param block_size: (optional)
               The block size to use for file reads and writes
           :param max_requests: (optional)
               The maximum number of parallel read or write requests
           :param progress_handler: (optional)
               The function to call to report upload progress
           :param error_handler: (optional)
               The function to call when an error occurs
           :type localpaths:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`,
               or a sequence of these
           :type remotepath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type preserve: `bool`
           :type recurse: `bool`
           :type follow_symlinks: `bool`
           :type sparse: `bool`
           :type block_size: `int`
           :type max_requests: `int`
           :type progress_handler: `callable`
           :type error_handler: `callable`

           :raises: | :exc:`OSError` if a local file I/O error occurs
                    | :exc:`Exception` if the server returns an error

        """

        await self._begin_copy(local_fs, self, localpaths, remotepath, 'put',
                               False, preserve, recurse, follow_symlinks,
                               sparse, block_size, max_requests,
                               progress_handler, error_handler)

    async def copy(self, srcpaths: bytes | str | PurePath,
                   dstpath: Optional[bytes | str | PurePath] = None, *,
                   preserve: bool = False, recurse: bool = False,
                   follow_symlinks: bool = False, sparse: bool = True,
                   block_size: int = -1, max_requests: int = -1,
                   progress_handler: SFTPProgressHandler = None,
                   error_handler: ExceptionHandler = None,
                   remote_only: bool = False) -> None:
        """Copy remote files to a new location

           This method copies one or more files or directories on the
           remote system to a new location. Either a single source path
           or a sequence of source paths to copy can be provided.

           When copying a single file or directory, the destination path
           can be either the full path to copy data into or the path to
           an existing directory where the data should be placed. In the
           latter case, the base file name from the source path will be
           used as the destination name.

           When copying multiple files, the destination path must refer
           to an existing remote directory.

           If no destination path is provided, the file is copied into
           the current remote working directory.

           If preserve is `True`, the access and modification times
           and permissions of the original file are set on the
           copied file.

           If recurse is `True` and the source path points at a
           directory, the entire subtree under that directory is
           copied.

           If follow_symlinks is set to `True`, symbolic links found
           in the source will have the contents of their target copied
           rather than creating a copy of the symbolic link. When
           using this option during a recursive copy, one needs to
           watch out for links that result in loops.

           The block_size argument specifies the size of read and write
           requests issued when copying the files, defaulting to the
           maximum allowed by the server, or 16 KB if the server
           doesn't advertise limits.

           The max_requests argument specifies the maximum number of
           parallel read or write requests issued, defaulting to a
           value between 16 and 128 depending on the selected block
           size to avoid excessive memory usage.

           If progress_handler is specified, it will be called after
           each block of a file is successfully copied. The arguments
           passed to this handler will be the source path, destination
           path, bytes copied so far, and total bytes in the file
           being copied. If multiple source paths are provided or
           recurse is set to `True`, the progress_handler will be
           called consecutively on each file being copied.

           If error_handler is specified and an error occurs during
           the copy, this handler will be called with the exception
           instead of it being raised. This is intended to primarily be
           used when multiple source paths are provided or when recurse
           is set to `True`, to allow error information to be collected
           without aborting the copy of the remaining files. The error
           handler can raise an exception if it wants the copy to
           completely stop. Otherwise, after an error, the copy will
           continue starting with the next file.

           :param srcpaths:
               The paths of the remote files or directories to copy
           :param dstpath: (optional)
               The path of the remote file or directory to copy into
           :param preserve: (optional)
               Whether or not to preserve the original file attributes
           :param recurse: (optional)
               Whether or not to recursively copy directories
           :param follow_symlinks: (optional)
               Whether or not to follow symbolic links
           :param sparse: (optional)
               Whether or not to do a sparse file copy where it is supported
           :param block_size: (optional)
               The block size to use for file reads and writes
           :param max_requests: (optional)
               The maximum number of parallel read or write requests
           :param progress_handler: (optional)
               The function to call to report copy progress
           :param error_handler: (optional)
               The function to call when an error occurs
           :param remote_only: (optional)
               Whether or not to only allow this to be a remote copy
           :type srcpaths:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`,
               or a sequence of these
           :type dstpath:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type preserve: `bool`
           :type recurse: `bool`
           :type follow_symlinks: `bool`
           :type sparse: `bool`
           :type block_size: `int`
           :type max_requests: `int`
           :type progress_handler: `callable`
           :type error_handler: `callable`
           :type remote_only: `bool`

           :raises: | :exc:`OSError` if a local file I/O error occurs
                    | :exc:`Exception` if the server returns an error

        """

        await self._begin_copy(self, self, srcpaths, dstpath, 'remote copy',
                               False, preserve, recurse, follow_symlinks,
                               sparse, block_size, max_requests,
                               progress_handler, error_handler, remote_only)

    async def mget(self, remotepaths: bytes | str | PurePath,
                   localpath: Optional[bytes | str | PurePath] = None, *,
                   preserve: bool = False, recurse: bool = False,
                   follow_symlinks: bool = False, sparse: bool = True,
                   block_size: int = -1, max_requests: int = -1,
                   progress_handler: SFTPProgressHandler = None,
                   error_handler: ExceptionHandler = None) -> None:
        """Download remote files with glob pattern match

           This method downloads files and directories from the remote
           system matching one or more glob patterns.

           The arguments to this method are identical to the :meth:`get`
           method, except that the remote paths specified can contain
           wildcard patterns.

        """

        await self._begin_copy(self, local_fs, remotepaths, localpath, 'mget',
                               True, preserve, recurse, follow_symlinks,
                               sparse, block_size, max_requests,
                               progress_handler, error_handler)

    async def mput(self, localpaths: bytes | str | PurePath,
                   remotepath: Optional[bytes | str | PurePath] = None, *,
                   preserve: bool = False, recurse: bool = False,
                   follow_symlinks: bool = False, sparse: bool = True,
                   block_size: int = -1, max_requests: int = -1,
                   progress_handler: SFTPProgressHandler = None,
                   error_handler: ExceptionHandler = None) -> None:
        """Upload local files with glob pattern match

           This method uploads files and directories to the remote
           system matching one or more glob patterns.

           The arguments to this method are identical to the :meth:`put`
           method, except that the local paths specified can contain
           wildcard patterns.

        """

        await self._begin_copy(local_fs, self, localpaths, remotepath, 'mput',
                               True, preserve, recurse, follow_symlinks,
                               sparse, block_size, max_requests,
                               progress_handler, error_handler)

    async def mcopy(self, srcpaths: bytes | str | PurePath,
                    dstpath: Optional[bytes | str | PurePath] = None, *,
                    preserve: bool = False, recurse: bool = False,
                    follow_symlinks: bool = False, sparse: bool = True,
                    block_size: int = -1, max_requests: int = -1,
                    progress_handler: SFTPProgressHandler = None,
                    error_handler: ExceptionHandler = None,
                    remote_only: bool = False) -> None:
        """Copy remote files with glob pattern match

           This method copies files and directories on the remote
           system matching one or more glob patterns.

           The arguments to this method are identical to the :meth:`copy`
           method, except that the source paths specified can contain
           wildcard patterns.

        """

        await self._begin_copy(self, self, srcpaths, dstpath, 'remote mcopy',
                               True, preserve, recurse, follow_symlinks,
                               sparse, block_size, max_requests,
                               progress_handler, error_handler, remote_only)

    async def remote_copy(self, src: _SFTPClientFileOrPath,
                          dst: _SFTPClientFileOrPath, src_offset: int = 0,
                          src_length: int = 0, dst_offset: int = 0) -> None:
        """Copy data between remote files

           :param src:
               The remote file object to read data from
           :param dst:
               The remote file object to write data to
           :param src_offset: (optional)
               The offset to begin reading data from
           :param src_length: (optional)
               The number of bytes to attempt to copy
           :param dst_offset: (optional)
               The offset to begin writing data to
           :type src:
               :class:`SFTPClientFile`, :class:`PurePath <pathlib.PurePath>`,
               `str`, or `bytes`
           :type dst:
               :class:`SFTPClientFile`, :class:`PurePath <pathlib.PurePath>`,
               `str`, or `bytes`
           :type src_offset: `int`
           :type src_length: `int`
           :type dst_offset: `int`

           :raises: :exc:`Exception` if the server doesn't support this
                    extension or returns an error

        """

        if isinstance(src, (bytes, str, PurePath)):
            src = await self.open(src, 'rb', block_size=0)

        if isinstance(dst, (bytes, str, PurePath)):
            dst = await self.open(dst, 'wb', block_size=0)

        await self._handler.copy_data(src.handle, src_offset, src_length,
                                      dst.handle, dst_offset)

    async def glob(self, patterns: bytes | str | PurePath,
                   error_handler: ExceptionHandler = None) -> \
            Sequence[str | bytes]:
        """Match remote files against glob patterns

           This method matches remote files against one or more glob
           patterns. Either a single pattern or a sequence of patterns
           can be provided to match against.

           Supported wildcard characters include '*', '?', and
           character ranges in square brackets. In addition, '**'
           can be used to trigger a recursive directory search at
           that point in the pattern, and a trailing slash can be
           used to request that only directories get returned.

           If error_handler is specified and an error occurs during
           the match, this handler will be called with the exception
           instead of it being raised. This is intended to primarily be
           used when multiple patterns are provided to allow error
           information to be collected without aborting the match
           against the remaining patterns. The error handler can raise
           an exception if it wants to completely abort the match.
           Otherwise, after an error, the match will continue starting
           with the next pattern.

           An error will be raised if any of the patterns completely
           fail to match, and this can either stop the match against
           the remaining patterns or be handled by the error_handler
           just like other errors.

           :param patterns:
               Glob patterns to try and match remote files against
           :param error_handler: (optional)
               The function to call when an error occurs
           :type patterns:
               :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`,
               or a sequence of these
           :type error_handler: `callable`

           :raises: :exc:`Exception` if the server returns an error
                    or no match is found

        """

        return [name.filename for name in
                await self.glob_sftpname(patterns, error_handler)]

    async def glob_sftpname(self, patterns: bytes | str | PurePath,
                            error_handler: ExceptionHandler = None) -> \
            Sequence[SFTPName]:
        """Match glob patterns and return SFTPNames

           This method is similar to :meth:`glob`, but it returns matching
           file names and attributes as :class:`SFTPName` objects.

        """

        if isinstance(patterns, (bytes, str, PurePath)):
            patterns = [patterns]

        glob = SFTPGlob(self, len(patterns) > 1)
        matches: List[SFTPName] = []

        for pattern in patterns:
            new_matches = await glob.match(self.encode(pattern),
                                           error_handler, self.version)

            if isinstance(pattern, (str, PurePath)):
                for name in new_matches:
                    name.filename = self.decode(cast(bytes, name.filename))

            matches.extend(new_matches)

        return matches

    async def makedirs(self, path: bytes | str | PurePath, attrs: SFTPAttrs = SFTPAttrs(),
                       exist_ok: bool = False) -> None:
        """Create a remote directory with the specified attributes

           This method creates a remote directory at the specified path
           similar to :meth:`mkdir`, but it will also create any
           intermediate directories which don't yet exist.

           If the target directory already exists and exist_ok is set
           to `False`, this method will raise an error.

           :param path:
               The path of where the new remote directory should be created
           :param attrs: (optional)
               The file attributes to use when creating the directory or
               any intermediate directories
           :param exist_ok: (optional)
               Whether or not to raise an error if thet target directory
               already exists
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type attrs: :class:`SFTPAttrs`
           :type exist_ok: `bool`

           :raises: :exc:`Exception` if the server returns an error

        """

        path = self.encode(path)
        curpath = b'/' if posixpath.isabs(path) else (self._cwd or b'')
        exists = True
        parts = path.split(b'/')
        last = len(parts) - 1

        exc: Type[Exception]

        for i, part in enumerate(parts):
            curpath = posixpath.join(curpath, part)

            try:
                await self.mkdir(curpath, attrs)
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

    async def rmtree(self, path: bytes | str | PurePath, ignore_errors: bool = False,
                     onerror: _SFTPOnErrorHandler = None) -> None:
        """Recursively delete a directory tree

           This method removes all the files in a directory tree.

           If ignore_errors is set, errors are ignored. Otherwise,
           if onerror is set, it will be called with arguments of
           the function which failed, the path it failed on, and
           exception information returns by :func:`sys.exc_info()`.

           If follow_symlinks is set, files or directories pointed at by
           symlinks (and their subdirectories, if any) will be removed
           in addition to the links pointing at them.

           :param path:
               The path of the parent directory to remove
           :param ignore_errors: (optional)
               Whether or not to ignore errors during the remove
           :param onerror: (optional)
               A function to call when errors occur
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type ignore_errors: `bool`
           :type onerror: `callable`

           :raises: :exc:`Exception` if the server returns an error

        """

        async def _unlink(path: bytes) -> None:
            """Internal helper for unlinking non-directories"""

            assert onerror is not None

            try:
                await self.unlink(path)
            except Exception:
                onerror(self.unlink, path, sys.exc_info())

        async def _rmtree(path: bytes) -> None:
            """Internal helper for rmtree recursion"""

            assert onerror is not None

            tasks = []

            try:
                async with sem:
                    async for entry in self.scandir(path):
                        filename = cast(bytes, entry.filename)

                        if filename in (b'.', b'..'):
                            continue

                        filename = posixpath.join(path, filename)

                        if entry.attrs.type == FILEXFER_TYPE_DIRECTORY:
                            task = _rmtree(filename)
                        else:
                            task = _unlink(filename)

                        tasks.append(asyncio.ensure_future(task))
            except Exception:
                onerror(self.scandir, path, sys.exc_info())

            results = await asyncio.gather(*tasks, return_exceptions=True)
            exc = next((result for result in results
                        if isinstance(result, Exception)), None)

            if exc:
                raise exc

            try:
                await self.rmdir(path)
            except Exception:
                onerror(self.rmdir, path, sys.exc_info())

        # pylint: disable=function-redefined
        if ignore_errors:
            def onerror(*_args: object) -> None:
                pass
        elif onerror is None:
            def onerror(*_args: object) -> None:
                raise # pylint: disable=misplaced-bare-raise
        # pylint: enable=function-redefined

        assert onerror is not None

        path = self.encode(path)
        sem = asyncio.Semaphore(_MAX_SFTP_REQUESTS)

        try:
            if await self.islink(path):
                raise SFTPNoSuchFile(path.decode('utf-8', 'backslashreplace') +
                                     ' must not be a symlink')
        except Exception:
            onerror(self.islink, path, sys.exc_info())
            return

        await _rmtree(path)

    @async_context_manager
    async def open(self, path: bytes | str | PurePath,
                   pflags_or_mode: Union[int, str] = FXF_READ,
                   attrs: SFTPAttrs = SFTPAttrs(),
                   encoding: Optional[str] = 'utf-8', errors: str = 'strict',
                   block_size: int = -1,
                   max_requests: int = -1) -> SFTPClientFile:
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
                    | :exc:`Exception` if the server returns an error

        """

        if isinstance(pflags_or_mode, str):
            pflags, binary = _mode_to_pflags(pflags_or_mode)

            if binary:
                encoding = None
        else:
            pflags = pflags_or_mode

        path = self.compose_path(path)
        handle = await self._handler.open(path, pflags, attrs)

        return SFTPClientFile(self._handler, handle, bool(pflags & FXF_APPEND),
                              encoding, errors, block_size, max_requests)

    @async_context_manager
    async def open56(self, path: bytes | str | PurePath,
                     desired_access: int = ACE4_READ_DATA |
                                           ACE4_READ_ATTRIBUTES,
                     flags: int = FXF_OPEN_EXISTING,
                     attrs: SFTPAttrs = SFTPAttrs(),
                     encoding: Optional[str] = 'utf-8', errors: str = 'strict',
                     block_size: int = -1,
                     max_requests: int = -1) -> SFTPClientFile:
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
                    | :exc:`Exception` if the server returns an error

        """

        path = self.compose_path(path)
        handle = await self._handler.open56(path, desired_access, flags, attrs)

        return SFTPClientFile(self._handler, handle,
                              bool(desired_access & ACE4_APPEND_DATA or
                                   flags & FXF_APPEND_DATA),
                              encoding, errors, block_size, max_requests)

    async def stat(self, path: bytes | str | PurePath, flags = FILEXFER_ATTR_DEFINED_V4, *,
                   follow_symlinks: bool = True) -> SFTPAttrs:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        path = self.compose_path(path)
        return await self._handler.stat(path, flags,
                                        follow_symlinks=follow_symlinks)

    async def lstat(self, path: bytes | str | PurePath,
                    flags = FILEXFER_ATTR_DEFINED_V4) -> SFTPAttrs:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        path = self.compose_path(path)
        return await self._handler.lstat(path, flags)

    async def setstat(self, path: bytes | str | PurePath, attrs: SFTPAttrs, *,
                      follow_symlinks: bool = True) -> None:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        path = self.compose_path(path)

        await self._handler.setstat(path, attrs,
                                    follow_symlinks=follow_symlinks)

    async def statvfs(self, path: bytes | str | PurePath) -> SFTPVFSAttrs:
        """Get attributes of a remote file system

           This method queries the attributes of the file system containing
           the specified path.

           :param path:
               The path of the remote file system to get attributes for
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :returns: An :class:`SFTPVFSAttrs` containing the file system
                     attributes

           :raises: :exc:`Exception` if the server doesn't support this
                    extension or returns an error

        """

        path = self.compose_path(path)
        return await self._handler.statvfs(path)

    async def truncate(self, path: bytes | str | PurePath, size: int) -> None:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        await self.setstat(path, SFTPAttrs(size=size))

    @overload
    async def chown(self, path: bytes | str | PurePath, uid: int, gid: int, *,
                    follow_symlinks: bool = True) -> \
        None: ... # pragma: no cover

    @overload
    async def chown(self, path: bytes | str | PurePath, owner: str, group: str, *,
                    follow_symlinks: bool = True) -> \
        None: ... # pragma: no cover

    async def chown(self, path, uid_or_owner = None, gid_or_group = None,
                    uid = None, gid = None, owner = None, group = None, *,
                    follow_symlinks = True):
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

           :raises: :exc:`Exception` if the server returns an error

        """

        if isinstance(uid_or_owner, int):
            uid = uid_or_owner
        elif isinstance(uid_or_owner, str):
            owner = uid_or_owner

        if isinstance(gid_or_group, int):
            gid = gid_or_group
        elif isinstance(gid_or_group, str):
            group = gid_or_group

        await self.setstat(path, SFTPAttrs(uid=uid, gid=gid,
                                           owner=owner, group=group),
                           follow_symlinks=follow_symlinks)

    async def chmod(self, path: bytes | str | PurePath, mode: int, *,
                    follow_symlinks: bool = True) -> None:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        await self.setstat(path, SFTPAttrs(permissions=mode),
                           follow_symlinks=follow_symlinks)

    async def utime(self, path: bytes | str | PurePath,
                    times: Optional[Tuple[float, float]] = None,
                    ns: Optional[Tuple[int, int]] = None, *,
                    follow_symlinks: bool = True) -> None:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        await self.setstat(path, _utime_to_attrs(times, ns),
                           follow_symlinks=follow_symlinks)

    async def exists(self, path: bytes | str | PurePath) -> bool:
        """Return if the remote path exists and isn't a broken symbolic link

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        return await self._type(path) != FILEXFER_TYPE_UNKNOWN

    async def lexists(self, path: bytes | str | PurePath) -> bool:
        """Return if the remote path exists, without following symbolic links

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        return await self._type(path, statfunc=self.lstat) != \
            FILEXFER_TYPE_UNKNOWN

    async def getatime(self, path: bytes | str | PurePath) -> Optional[float]:
        """Return the last access time of a remote file or directory

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        attrs = await self.stat(path)

        return _tuple_to_float_sec(attrs.atime, attrs.atime_ns) \
            if attrs.atime is not None else None

    async def getatime_ns(self, path: bytes | str | PurePath) -> Optional[int]:
        """Return the last access time of a remote file or directory

           The time returned is nanoseconds since the epoch.

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        attrs = await self.stat(path)

        return _tuple_to_nsec(attrs.atime, attrs.atime_ns) \
            if attrs.atime is not None else None

    async def getcrtime(self, path: bytes | str | PurePath) -> Optional[float]:
        """Return the creation time of a remote file or directory (SFTPv4 only)

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        attrs = await self.stat(path)

        return _tuple_to_float_sec(attrs.crtime, attrs.crtime_ns) \
            if attrs.crtime is not None else None

    async def getcrtime_ns(self, path: bytes | str | PurePath) -> Optional[int]:
        """Return the creation time of a remote file or directory

           The time returned is nanoseconds since the epoch.

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        attrs = await self.stat(path)

        return _tuple_to_nsec(attrs.crtime, attrs.crtime_ns) \
            if attrs.crtime is not None else None

    async def getmtime(self, path: bytes | str | PurePath) -> Optional[float]:
        """Return the last modification time of a remote file or directory

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        attrs = await self.stat(path)

        return _tuple_to_float_sec(attrs.mtime, attrs.mtime_ns) \
            if attrs.mtime is not None else None

    async def getmtime_ns(self, path: bytes | str | PurePath) -> Optional[int]:
        """Return the last modification time of a remote file or directory

           The time returned is nanoseconds since the epoch.

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        attrs = await self.stat(path)

        return _tuple_to_nsec(attrs.mtime, attrs.mtime_ns) \
            if attrs.mtime is not None else None

    async def getsize(self, path: bytes | str | PurePath) -> Optional[int]:
        """Return the size of a remote file or directory

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        return (await self.stat(path)).size

    async def isdir(self, path: bytes | str | PurePath) -> bool:
        """Return if the remote path refers to a directory

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        return await self._type(path) == FILEXFER_TYPE_DIRECTORY

    async def isfile(self, path: bytes | str | PurePath) -> bool:
        """Return if the remote path refers to a regular file

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        return await self._type(path) == FILEXFER_TYPE_REGULAR

    async def islink(self, path: bytes | str | PurePath) -> bool:
        """Return if the remote path refers to a symbolic link

           :param path:
               The remote path to check
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        return await self._type(path, statfunc=self.lstat) == \
            FILEXFER_TYPE_SYMLINK

    async def remove(self, path: bytes | str | PurePath) -> None:
        """Remove a remote file

           This method removes a remote file or symbolic link.

           :param path:
               The path of the remote file or link to remove
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        path = self.compose_path(path)
        await self._handler.remove(path)

    async def unlink(self, path: bytes | str | PurePath) -> None:
        """Remove a remote file (see :meth:`remove`)"""

        await self.remove(path)

    async def rename(self, oldpath: bytes | str | PurePath, newpath: bytes | str | PurePath,
                     flags: int = 0) -> None:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        oldpath = self.compose_path(oldpath)
        newpath = self.compose_path(newpath)
        await self._handler.rename(oldpath, newpath, flags)

    async def posix_rename(self, oldpath: bytes | str | PurePath,
                           newpath: bytes | str | PurePath) -> None:
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

           :raises: :exc:`Exception` if the server doesn't support this
                    extension or returns an error

        """

        oldpath = self.compose_path(oldpath)
        newpath = self.compose_path(newpath)
        await self._handler.posix_rename(oldpath, newpath)

    async def scandir(self, path: bytes | str | PurePath = '.') -> AsyncIterator[SFTPName]:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        dirpath = self.compose_path(path)
        handle = await self._handler.opendir(dirpath)
        at_end = False

        try:
            while not at_end:
                names, at_end = await self._handler.readdir(handle)

                for entry in names:
                    if isinstance(path, (str, PurePath)):
                        entry.filename = \
                            self.decode(cast(bytes, entry.filename))

                        if entry.longname is not None:
                            entry.longname = \
                                self.decode(cast(bytes, entry.longname))

                    yield entry
        except SFTPEOFError:
            pass
        finally:
            await self._handler.close(handle)

    async def readdir(self, path: bytes | str | PurePath = '.') -> Sequence[SFTPName]:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        return [entry async for entry in self.scandir(path)]

    @overload
    async def listdir(self, path: bytes) -> \
        Sequence[bytes]: ... # pragma: no cover

    @overload
    async def listdir(self, path: str | PurePath = ...) -> \
        Sequence[str]: ... # pragma: no cover

    async def listdir(self, path: bytes | str | PurePath = '.') -> Sequence[str | bytes]:
        """Read the names of the files in a remote directory

           This method reads the names of files and subdirectories
           in a remote directory. If no path is provided, it defaults
           to the current remote working directory.

           :param path: (optional)
               The path of the remote directory to read
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :returns: A list of file/subdirectory names, as a `str` or `bytes`
                     matching the type used to pass in the path

           :raises: :exc:`Exception` if the server returns an error

        """

        names = await self.readdir(path)
        return [name.filename for name in names]

    async def mkdir(self, path: bytes | str | PurePath,
                    attrs: SFTPAttrs = SFTPAttrs()) -> None:
        """Create a remote directory with the specified attributes

           This method creates a new remote directory at the
           specified path with the requested attributes.

           :param path:
               The path of where the new remote directory should be created
           :param attrs: (optional)
               The file attributes to use when creating the directory
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`
           :type attrs: :class:`SFTPAttrs`

           :raises: :exc:`Exception` if the server returns an error

        """

        path = self.compose_path(path)
        await self._handler.mkdir(path, attrs)

    async def rmdir(self, path: bytes | str | PurePath) -> None:
        """Remove a remote directory

           This method removes a remote directory. The directory
           must be empty for the removal to succeed.

           :param path:
               The path of the remote directory to remove
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        path = self.compose_path(path)
        await self._handler.rmdir(path)

    @overload
    async def realpath(self, path: bytes, # pragma: no cover
                       *compose_paths: bytes) -> bytes: ...

    @overload
    async def realpath(self, path: str | PurePath, # pragma: no cover
                       *compose_paths: str | PurePath) -> str: ...

    @overload
    async def realpath(self, path: bytes, # pragma: no cover
                       *compose_paths: bytes, check: int) -> SFTPName: ...

    @overload
    async def realpath(self, path: str | PurePath, # pragma: no cover
                       *compose_paths: str | PurePath, check: int) -> SFTPName: ...

    async def realpath(self, path: bytes | str | PurePath, *compose_paths: bytes | str | PurePath,
                       check: int = FXRP_NO_CHECK) -> \
            str | bytes | SFTPName:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        if compose_paths and isinstance(compose_paths[-1], int):
            check = compose_paths[-1]
            compose_paths = compose_paths[:-1]

        path_bytes = self.compose_path(path)

        if self.version >= 6:
            names, _ = await self._handler.realpath(
                path_bytes, *map(self.encode, compose_paths), check=check)
        else:
            for cpath in compose_paths:
                path_bytes = self.compose_path(cpath, path_bytes)

            names, _ = await self._handler.realpath(path_bytes)

        if len(names) > 1:
            raise Exception('Too many names returned')

        if check != FXRP_NO_CHECK:
            if self.version < 6:
                try:
                    names[0].attrs = await self._handler.stat(
                        self.encode(names[0].filename),
                        _valid_attr_flags[self.version])
                except Exception:
                    if check == FXRP_STAT_IF_EXISTS:
                        names[0].attrs = SFTPAttrs(type=FILEXFER_TYPE_UNKNOWN)
                    else:
                        raise

            return names[0]
        else:
            return self.decode(cast(bytes, names[0].filename),
                               isinstance(path, (str, PurePath)))

    async def getcwd(self) -> str | bytes:
        """Return the current remote working directory

           :returns: The current remote working directory, decoded using
                     the specified path encoding

           :raises: :exc:`Exception` if the server returns an error

        """

        if self._cwd is None:
            self._cwd = await self.realpath(b'.')

        return self.decode(self._cwd)

    async def chdir(self, path: bytes | str | PurePath) -> None:
        """Change the current remote working directory

           :param path:
               The path to set as the new remote working directory
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        self._cwd = await self.realpath(self.encode(path))

    @overload
    async def readlink(self, path: bytes) -> bytes: ... # pragma: no cover

    @overload
    async def readlink(self, path: str | PurePath) -> str: ... # pragma: no cover

    async def readlink(self, path: bytes | str | PurePath) -> str | bytes:
        """Return the target of a remote symbolic link

           This method returns the target of a symbolic link.

           :param path:
               The path of the remote symbolic link to follow
           :type path: :class:`PurePath <pathlib.PurePath>`, `str`, or `bytes`

           :returns: The target path of the link as a `str` or `bytes`

           :raises: :exc:`Exception` if the server returns an error

        """

        linkpath = self.compose_path(path)
        names, _ = await self._handler.readlink(linkpath)

        if len(names) > 1:
            raise Exception('Too many names returned')

        return self.decode(cast(bytes, names[0].filename),
                           isinstance(path, (str, PurePath)))

    async def symlink(self, oldpath: bytes | str | PurePath, newpath: bytes | str | PurePath) -> None:
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

           :raises: :exc:`Exception` if the server returns an error

        """

        oldpath = self.encode(oldpath)
        newpath = self.compose_path(newpath)
        await self._handler.symlink(oldpath, newpath)

    async def link(self, oldpath: bytes | str | PurePath, newpath: bytes | str | PurePath) -> None:
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

           :raises: :exc:`Exception` if the server doesn't support this
                    extension or returns an error

        """

        oldpath = self.compose_path(oldpath)
        newpath = self.compose_path(newpath)
        await self._handler.link(oldpath, newpath)

    def exit(self) -> None:
        """Exit the SFTP client session

           This method exits the SFTP client session, closing the
           corresponding channel opened on the server.

        """

        self._handler.exit()

    async def wait_closed(self) -> None:
        """Wait for this SFTP client session to close"""

        await self._handler.wait_closed()
