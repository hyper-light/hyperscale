from typing import (
    Sequence,
    Callable,
)
from .constants import (
    DEFAULT_LANG,
    FX_OK,
    FX_NOT_A_DIRECTORY,
    FX_NO_CONNECTION,
    FX_BAD_MESSAGE,
    FX_BYTE_RANGE_LOCK_CONFLICT,
    FX_BYTE_RANGE_LOCK_REFUSED,
    FX_CANNOT_DELETE,
    FX_CONNECTION_LOST,
    FX_DELETE_PENDING,
    FX_DIR_NOT_EMPTY,
    FX_EOF,
    FX_FAILURE,
    FX_FILE_ALREADY_EXISTS,
    FX_FILE_CORRUPT,
    FX_FILE_IS_A_DIRECTORY,
    FX_GROUP_INVALID,
    FX_INVALID_FILENAME,
    FX_INVALID_HANDLE,
    FX_INVALID_PARAMETER,
    FX_LINK_LOOP,
    FX_LOCK_CONFLICT,
    FX_NO_MATCHING_BYTE_RANGE_LOCK,
    FX_NO_MEDIA,
    FX_NO_SPACE_ON_FILESYSTEM,
    FX_NO_SUCH_FILE,
    FX_NO_SUCH_PATH,
    FX_OP_UNSUPPORTED,
    FX_OWNER_INVALID,
    FX_PERMISSION_DENIED,
    FX_QUOTA_EXCEEDED,
    FX_UNKNOWN_PRINCIPAL,
    FX_V3_END,
    FX_V4_END,
    FX_V5_END,
    FX_V6_END,
    FX_WRITE_PROTECT,
    DISC_COMPRESSION_ERROR,
    DISC_CONNECTION_LOST,
    DISC_PROTOCOL_ERROR,
    DISC_SERVICE_NOT_AVAILABLE,
    DISC_ILLEGAL_USER_NAME,
    DISC_NO_MORE_AUTH_METHODS_AVAILABLE,
)

from .packet import SSHPacket, String, UInt32


class Error(Exception):
    """General SSH error"""

    def __init__(self, code: int, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(reason)
        self.code = code
        self.reason = reason
        self.lang = lang

class ChannelOpenError(Error):
    """SSH channel open error

       This exception is raised by connection handlers to report
       channel open failures.

       :param code:
           Channel open failure reason, taken from :ref:`channel open
           failure reason codes <ChannelOpenFailureReasons>`
       :param reason:
           A human-readable reason for the channel open failure
       :param lang:
           The language the reason is in
       :type code: `int`
       :type reason: `str`
       :type lang: `str`

    """

class PacketDecodeError(ValueError):
    """Packet decoding error"""


class SFTPError(Error):
    """SFTP error

       This exception is raised when an error occurs while processing
       an SFTP request. Exception codes should be taken from
       :ref:`SFTP error codes <SFTPErrorCodes>`.

       :param code:
           Disconnect reason, taken from :ref:`disconnect reason
           codes <DisconnectReasons>`
       :param reason:
           A human-readable reason for the disconnect
       :param lang: (optional)
           The language the reason is in
       :type code: `int`
       :type reason: `str`
       :type lang: `str`

    """

    @staticmethod
    def construct(packet: SSHPacket) -> 'SFTPError' | None:
        """Construct an SFTPError from an FXP_STATUS response"""

        code = packet.get_uint32()

        if packet:
            try:
                reason = packet.get_string().decode('utf-8')
                lang = packet.get_string().decode('ascii')
            except UnicodeDecodeError:
                raise SFTPBadMessage('Invalid status message') from None
        else:
            # Some servers may not always send reason and lang (usually
            # when responding with FX_OK). Tolerate this, automatically
            # filling in empty strings for them if they're not present.

            reason = ''
            lang = ''

        if code == FX_OK:
            return None
        else:
            try:
                exc = _sftp_error_map[code](reason, lang)
            except KeyError:
                exc = SFTPError(code, f'{reason} (error {code})', lang)

            exc.decode(packet)
            return exc

    def encode(self, version: int) -> bytes:
        """Encode an SFTPError as bytes in an SSHPacket"""

        if self.code == FX_NOT_A_DIRECTORY and version < 6:
            code = FX_NO_SUCH_FILE
        elif (self.code <= FX_V6_END and
                ((self.code > FX_V3_END and version <= 3) or
                 (self.code > FX_V4_END and version <= 4) or
                 (self.code > FX_V5_END and version <= 5))):
            code = FX_FAILURE
        else:
            code = self.code

        return UInt32(code) + String(self.reason) + String(self.lang)

    def decode(self, packet: SSHPacket) -> None:
        """Decode error-specific data"""

        # pylint: disable=no-self-use

        # By default, expect no error-specific data


class SFTPEOFError(SFTPError):
    """SFTP EOF error

       This exception is raised when end of file is reached when
       reading a file or directory.

       :param reason: (optional)
           Details about the EOF
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str = '', lang: str = DEFAULT_LANG):
        super().__init__(FX_EOF, reason, lang)


class SFTPNoSuchFile(SFTPError):
    """SFTP no such file

       This exception is raised when the requested file is not found.

       :param reason:
           Details about the missing file
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_NO_SUCH_FILE, reason, lang)


class SFTPPermissionDenied(SFTPError):
    """SFTP permission denied

       This exception is raised when the permissions are not available
       to perform the requested operation.

       :param reason:
           Details about the invalid permissions
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_PERMISSION_DENIED, reason, lang)


class SFTPFailure(SFTPError):
    """SFTP failure

       This exception is raised when an unexpected SFTP failure occurs.

       :param reason:
           Details about the failure
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_FAILURE, reason, lang)


class SFTPBadMessage(SFTPError):
    """SFTP bad message

       This exception is raised when an invalid SFTP message is
       received.

       :param reason:
           Details about the invalid message
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_BAD_MESSAGE, reason, lang)


class SFTPNoConnection(SFTPError):
    """SFTP no connection

       This exception is raised when an SFTP request is made on a
       closed SSH connection.

       :param reason:
           Details about the closed connection
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_NO_CONNECTION, reason, lang)


class SFTPConnectionLost(SFTPError):
    """SFTP connection lost

       This exception is raised when the SSH connection is lost or
       closed while making an SFTP request.

       :param reason:
           Details about the connection failure
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_CONNECTION_LOST, reason, lang)


class SFTPOpUnsupported(SFTPError):
    """SFTP operation unsupported

       This exception is raised when the requested SFTP operation
       is not supported.

       :param reason:
           Details about the unsupported operation
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_OP_UNSUPPORTED, reason, lang)


class SFTPInvalidHandle(SFTPError):
    """SFTP invalid handle (SFTPv4+)

       This exception is raised when the handle provided is invalid.

       :param reason:
           Details about the invalid handle
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_INVALID_HANDLE, reason, lang)


class SFTPNoSuchPath(SFTPError):
    """SFTP no such path (SFTPv4+)

       This exception is raised when the requested path is not found.

       :param reason:
           Details about the missing path
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_NO_SUCH_PATH, reason, lang)


class SFTPFileAlreadyExists(SFTPError):
    """SFTP file already exists (SFTPv4+)

       This exception is raised when the requested file already exists.

       :param reason:
           Details about the existing file
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_FILE_ALREADY_EXISTS, reason, lang)


class SFTPWriteProtect(SFTPError):
    """SFTP write protect (SFTPv4+)

       This exception is raised when a write is attempted to a file
       on read-only or write protected media.

       :param reason:
           Details about the requested file
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_WRITE_PROTECT, reason, lang)


class SFTPNoMedia(SFTPError):
    """SFTP no media (SFTPv4+)

       This exception is raised when there is no media in the
       requested drive.

       :param reason:
           Details about the requested drive
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_NO_MEDIA, reason, lang)


class SFTPNoSpaceOnFilesystem(SFTPError):
    """SFTP no space on filesystem (SFTPv5+)

       This exception is raised when there is no space available
       on the filesystem a file is being written to.

       :param reason:
           Details about the filesystem which has filled up
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_NO_SPACE_ON_FILESYSTEM, reason, lang)


class SFTPQuotaExceeded(SFTPError):
    """SFTP quota exceeded (SFTPv5+)

       This exception is raised when the user's storage quota
       is exceeded.

       :param reason:
           Details about the exceeded quota
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_QUOTA_EXCEEDED, reason, lang)


class SFTPUnknownPrincipal(SFTPError):
    """SFTP unknown principal (SFTPv5+)

       This exception is raised when a file owner or group is
       not reocgnized.

       :param reason:
           Details about the unknown principal
       :param lang: (optional)
           The language the reason is in
       :param unknown_names: (optional)
           A list of unknown principal names
       :type reason: `str`
       :type lang: `str`
       :type unknown_names: list of `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG,
                 unknown_names: Sequence[str] = ()):
        super().__init__(FX_UNKNOWN_PRINCIPAL, reason, lang)
        self.unknown_names = unknown_names

    def encode(self, version: int) -> bytes:
        """Encode an SFTPUnknownPrincipal as bytes in an SSHPacket"""

        return super().encode(version) + \
            b''.join(String(name) for name in self.unknown_names)

    def decode(self, packet: SSHPacket) -> None:
        """Decode error-specific data"""

        self.unknown_names = []

        try:
            while packet:
                self.unknown_names.append(
                    packet.get_string().decode('utf-8'))
        except UnicodeDecodeError:
            raise SFTPBadMessage('Invalid status message') from None


class SFTPLockConflict(SFTPError):
    """SFTP lock conflict (SFTPv5+)

       This exception is raised when a requested lock is held by
       another process.

       :param reason:
           Details about the conflicting lock
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_LOCK_CONFLICT, reason, lang)


class SFTPDirNotEmpty(SFTPError):
    """SFTP directory not empty (SFTPv6+)

       This exception is raised when a directory is not empty.

       :param reason:
           Details about the non-empty directory
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_DIR_NOT_EMPTY, reason, lang)


class SFTPNotADirectory(SFTPError):
    """SFTP not a directory (SFTPv6+)

       This exception is raised when a specified file is
       not a directory where one was expected.

       :param reason:
           Details about the file expected to be a directory
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_NOT_A_DIRECTORY, reason, lang)


class SFTPInvalidFilename(SFTPError):
    """SFTP invalid filename (SFTPv6+)

       This exception is raised when a filename is not valid.

       :param reason:
           Details about the invalid filename
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_INVALID_FILENAME, reason, lang)


class SFTPLinkLoop(SFTPError):
    """SFTP link loop (SFTPv6+)

       This exception is raised when a symbolic link loop is detected.

       :param reason:
           Details about the link loop
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_LINK_LOOP, reason, lang)


class SFTPCannotDelete(SFTPError):
    """SFTP cannot delete (SFTPv6+)

       This exception is raised when a file cannot be deleted.

       :param reason:
           Details about the undeletable file
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_CANNOT_DELETE, reason, lang)


class SFTPInvalidParameter(SFTPError):
    """SFTP invalid parameter (SFTPv6+)

       This exception is raised when parameters in a request are
       out of range or incompatible with one another.

       :param reason:
           Details about the invalid parameter
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_INVALID_PARAMETER, reason, lang)


class SFTPFileIsADirectory(SFTPError):
    """SFTP file is a directory (SFTPv6+)

       This exception is raised when a specified file is a
       directory where one isn't allowed.

       :param reason:
           Details about the unexpected directory
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_FILE_IS_A_DIRECTORY, reason, lang)


class SFTPByteRangeLockConflict(SFTPError):
    """SFTP byte range lock conflict (SFTPv6+)

       This exception is raised when a read or write request overlaps
       a byte range lock held by another process.

       :param reason:
           Details about the conflicting byte range lock
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_BYTE_RANGE_LOCK_CONFLICT, reason, lang)


class SFTPByteRangeLockRefused(SFTPError):
    """SFTP byte range lock refused (SFTPv6+)

       This exception is raised when a request for a byte range
       lock was refused.

       :param reason:
           Details about the refused byte range lock
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_BYTE_RANGE_LOCK_REFUSED, reason, lang)


class SFTPDeletePending(SFTPError):
    """SFTP delete pending (SFTPv6+)

       This exception is raised when an operation was attempted
       on a file for which a delete operation is pending.
       another process.

       :param reason:
           Details about the file being deleted
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_DELETE_PENDING, reason, lang)


class SFTPFileCorrupt(SFTPError):
    """SFTP file corrupt (SFTPv6+)

       This exception is raised when filesystem corruption is detected.

       :param reason:
           Details about the corrupted filesystem
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_FILE_CORRUPT, reason, lang)


class SFTPOwnerInvalid(SFTPError):
    """SFTP owner invalid (SFTPv6+)

       This exception is raised when a principal cannot be assigned
       as the owner of a file.

       :param reason:
           Details about the principal being set as a file's owner
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_OWNER_INVALID, reason, lang)


class SFTPGroupInvalid(SFTPError):
    """SFTP group invalid (SFTPv6+)

       This exception is raised when a principal cannot be assigned
       as the primary group of a file.

       :param reason:
           Details about the principal being set as a file's group
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_GROUP_INVALID, reason, lang)


class SFTPNoMatchingByteRangeLock(SFTPError):
    """SFTP no matching byte range lock (SFTPv6+)

       This exception is raised when an unlock is requested for a
       byte range lock which is not currently held.

       :param reason:
           Details about the byte range lock being released
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(FX_NO_MATCHING_BYTE_RANGE_LOCK, reason, lang)




class DisconnectError(Error):
    """SSH disconnect error

       This exception is raised when a serious error occurs which causes
       the SSH connection to be disconnected. Exception codes should be
       taken from :ref:`disconnect reason codes <DisconnectReasons>`.
       See below for exception subclasses tied to specific disconnect
       reasons if you want to customize your handling by reason.

       :param code:
           Disconnect reason, taken from :ref:`disconnect reason
           codes <DisconnectReasons>`
       :param reason:
           A human-readable reason for the disconnect
       :param lang: (optional)
           The language the reason is in
       :type code: `int`
       :type reason: `str`
       :type lang: `str`

    """


class CompressionError(DisconnectError):
    """SSH compression error

       This exception is raised when an error occurs while compressing
       or decompressing data sent on the SSH connection.

       :param reason:
           Details about the compression error
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(DISC_COMPRESSION_ERROR, reason, lang)


class ConnectionLost(DisconnectError):
    """SSH connection lost

       This exception is raised when the SSH connection to the remote
       system is unexpectedly lost. It can also occur as a result of
       the remote system failing to respond to keepalive messages or
       as a result of a login timeout, when those features are enabled.

       :param reason:
           Details about the connection failure
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(DISC_CONNECTION_LOST, reason, lang)



class SoftEOFReceived(Exception):
    """SSH soft EOF request received

       This exception is raised on an SSH server stdin stream when the
       client sends an EOF from within the line editor on the channel.

    """

    def __init__(self) -> None:
        super().__init__('Soft EOF')



class ProtocolError(DisconnectError):
    """SSH protocol error

       This exception is raised when the SSH connection is disconnected
       due to an SSH protocol error being detected.

       :param reason:
           Details about the SSH protocol error detected
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(DISC_PROTOCOL_ERROR, reason, lang)



class KeyImportError(ValueError):
    """Key import error

       This exception is raised by key import functions when the
       data provided cannot be imported as a valid key.

    """


class ServiceNotAvailable(DisconnectError):
    """SSH service not available

       This exception is raised when an unexpected service name is
       received during the SSH handshake.

       :param reason:
           Details about the unexpected SSH service
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(DISC_SERVICE_NOT_AVAILABLE, reason, lang)


class PacketDecodeError(ValueError):
    """Packet decoding error"""


class IllegalUserName(DisconnectError):
    """SSH illegal user name

       This exception is raised when an error occurs while processing
       the username sent during the SSL handshake.

       :param reason:
           Details about the illegal username
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(DISC_ILLEGAL_USER_NAME, reason, lang)


class SASLPrepError(ValueError):
    """Invalid data provided to saslprep"""


class PermissionDenied(DisconnectError):
    """SSH permission denied

       This exception is raised when there are no authentication methods
       remaining to complete SSH client authentication.

       :param reason:
           Details about the SSH protocol error detected
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(DISC_NO_MORE_AUTH_METHODS_AVAILABLE, reason, lang)


class ChannelOpenError(Error):
    """SSH channel open error

       This exception is raised by connection handlers to report
       channel open failures.

       :param code:
           Channel open failure reason, taken from :ref:`channel open
           failure reason codes <ChannelOpenFailureReasons>`
       :param reason:
           A human-readable reason for the channel open failure
       :param lang:
           The language the reason is in
       :type code: `int`
       :type reason: `str`
       :type lang: `str`

    """


class ProtocolNotSupported(DisconnectError):
    """SSH protocol not supported

       This exception is raised when the remote system sends an SSH
       protocol version which is not supported.

       :param reason:
           Details about the unsupported SSH protocol version
       :param lang: (optional)
           The language the reason is in
       :type reason: `str`
       :type lang: `str`

    """

    def __init__(self, reason: str, lang: str = DEFAULT_LANG):
        super().__init__(DISC_PROTOCOL_ERROR, reason, lang)
        

_sftp_error_map: dict[int, Callable[[str, str], SFTPError]] = {
    FX_EOF: SFTPEOFError,
    FX_NO_SUCH_FILE: SFTPNoSuchFile,
    FX_PERMISSION_DENIED: SFTPPermissionDenied,
    FX_FAILURE: SFTPFailure,
    FX_BAD_MESSAGE: SFTPBadMessage,
    FX_NO_CONNECTION: SFTPNoConnection,
    FX_CONNECTION_LOST: SFTPConnectionLost,
    FX_OP_UNSUPPORTED: SFTPOpUnsupported,
    FX_INVALID_HANDLE: SFTPInvalidHandle,
    FX_NO_SUCH_PATH: SFTPNoSuchPath,
    FX_FILE_ALREADY_EXISTS: SFTPFileAlreadyExists,
    FX_WRITE_PROTECT: SFTPWriteProtect,
    FX_NO_MEDIA: SFTPNoMedia,
    FX_NO_SPACE_ON_FILESYSTEM: SFTPNoSpaceOnFilesystem,
    FX_QUOTA_EXCEEDED: SFTPQuotaExceeded,
    FX_UNKNOWN_PRINCIPAL: SFTPUnknownPrincipal,
    FX_LOCK_CONFLICT: SFTPLockConflict,
    FX_DIR_NOT_EMPTY: SFTPDirNotEmpty,
    FX_NOT_A_DIRECTORY: SFTPNotADirectory,
    FX_INVALID_FILENAME: SFTPInvalidFilename,
    FX_LINK_LOOP: SFTPLinkLoop,
    FX_CANNOT_DELETE: SFTPCannotDelete,
    FX_INVALID_PARAMETER: SFTPInvalidParameter,
    FX_FILE_IS_A_DIRECTORY: SFTPFileIsADirectory,
    FX_BYTE_RANGE_LOCK_CONFLICT: SFTPByteRangeLockConflict,
    FX_BYTE_RANGE_LOCK_REFUSED: SFTPByteRangeLockRefused,
    FX_DELETE_PENDING: SFTPDeletePending,
    FX_FILE_CORRUPT: SFTPFileCorrupt,
    FX_OWNER_INVALID: SFTPOwnerInvalid,
    FX_GROUP_INVALID: SFTPGroupInvalid,
    FX_NO_MATCHING_BYTE_RANGE_LOCK: SFTPNoMatchingByteRangeLock
}
