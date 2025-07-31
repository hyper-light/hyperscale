import asyncio
import functools
import inspect
import os
import socket
import sys
import time
from collections import OrderedDict
from typing import (
    Sequence,
    cast,
    Self,
    Type,
    Awaitable,
    AnyStr,
    Any,
    Callable,
    TYPE_CHECKING,
)
from .async_context_manager import async_context_manager
from .ssh_packet_handler import SSHPacketHandler
from .constants import (
    MSG_CHANNEL_FIRST,
    MSG_CHANNEL_LAST,
    MSG_CHANNEL_OPEN,
    MSG_CHANNEL_OPEN_CONFIRMATION,
    MSG_CHANNEL_OPEN_FAILURE,
    MSG_DEBUG,
    MSG_DISCONNECT,
    MSG_EXT_INFO,
    MSG_GLOBAL_REQUEST,
    MSG_IGNORE,
    MSG_KEX_FIRST,
    MSG_KEX_LAST,
    MSG_KEXINIT,
    MSG_NEWKEYS,
    MSG_REQUEST_FAILURE,
    MSG_REQUEST_SUCCESS,
    MSG_SERVICE_ACCEPT,
    MSG_SERVICE_REQUEST,
    MSG_UNIMPLEMENTED,
    MSG_USERAUTH_BANNER,
    MSG_USERAUTH_FAILURE,
    MSG_USERAUTH_FIRST,
    MSG_USERAUTH_LAST,
    MSG_USERAUTH_REQUEST,
    MSG_USERAUTH_SUCCESS,
    MAX_USERNAME_LEN,
    CONNECTION_SERVICE,
    CERT_TYPE_HOST,
    USERAUTH_SERVICE,
    DISC_BY_APPLICATION,
    DEFAULT_LANG,
    DEFAULT_WINDOW,
    DEFAULT_MAX_PKTSIZE,
    OPEN_CONNECT_FAILED,
    MAX_BANNER_LINE_LEN,
    MAX_BANNER_LINES,
    MAX_VERSION_LINE_LEN,
    OPEN_UNKNOWN_CHANNEL_TYPE,
    OPEN_ADMINISTRATIVELY_PROHIBITED,
)
from .encryption import Encryption
from .error import (
    KeyImportError,
    DisconnectError,
    ProtocolError,
    ServiceNotAvailable,
    PacketDecodeError,
    IllegalUserName,
    SASLPrepError,
    PermissionDenied,
    ChannelOpenError,
    ConnectionLost,
    ProtocolNotSupported,
)
from .packet import (
    UInt32,
    SSHPacket,
    String,
    Byte,
    Boolean,
    NameList,
    PacketHandler,
)
from .ssh_agent_channel import SSHAgentChannel
from .ssh_forwarder import SSHForwarder
from .ssh_public_key import SSHKey
from .ssh_tcp_channel import SSHTCPChannel, SSHTCPSession, SSHTCPSessionFactory
from .ssh_tun_tap_channel import SSHTunTapChannel
from .ssh_unix_channel import SSHUNIXChannel, SSHUNIXSession, SSHUNIXSessionFactory
from .ssh_x11_channel import SSHX11Channel
from .types import TracebackType, DataType, ListenKey, SockAddr


if TYPE_CHECKING:
    from .auth import ClientAuth
    from .ssh_channel import SSHChannel
    from .ssh_client_connection import SSHClientConnection
    from .ssh_listener import SSHListener


GlobalRequest = tuple[PacketHandler | None, SSHPacket, bool]
GlobalRequestResult = tuple[int, SSHPacket]

class SSHConnection(SSHPacketHandler, asyncio.Protocol):
    """Parent class for SSH connections"""

    _handler_names = get_symbol_names(globals(), 'MSG_')

    next_conn = 0    # Next connection number, for logging

    @staticmethod
    def _get_next_conn() -> int:
        """Return the next available connection number (for logging)"""

        next_conn = SSHConnection.next_conn
        SSHConnection.next_conn += 1
        return next_conn

    def __init__(self, loop: asyncio.AbstractEventLoop,
                 options: 'SSHConnectionOptions',
                 acceptor: _AcceptHandler, error_handler: _ErrorHandler,
                 wait: str| None, server: bool):
        self._loop = loop
        self._options = options
        self._protocol_factory = options.protocol_factory
        self._acceptor = acceptor
        self._error_handler = error_handler
        self._server = server
        self._wait = wait
        self._waiter = options.waiter if wait else None

        self._transport: asyncio.Transport | None = None
        self._local_addr = ''
        self._local_port = 0
        self._peer_host = ''
        self._peer_addr = ''
        self._peer_port = 0
        self._tcp_keepalive = options.tcp_keepalive
        self._owner: SSHClient | None = None
        self._extra: dict[str, object] = {}

        self._inpbuf = b''
        self._packet = b''
        self._pktlen = 0
        self._banner_lines = 0

        self._version = options.version
        self._client_version = b''
        self._server_version = b''
        self._client_kexinit = b''
        self._server_kexinit = b''
        self._session_id = b''

        self._send_seq = 0
        self._send_encryption: Encryption | None = None
        self._send_enchdrlen = 5
        self._send_blocksize = 8
        self._compressor: Compressor | None = None
        self._compress_after_auth = False
        self._deferred_packets: list[tuple[int, Sequence[bytes]]] = []

        self._recv_handler = self._recv_version
        self._recv_seq = 0
        self._recv_encryption: Encryption | None = None
        self._recv_blocksize = 8
        self._recv_macsize = 0
        self._decompressor: Decompressor | None = None
        self._decompress_after_auth = False
        self._next_recv_encryption: Encryption | None = None
        self._next_recv_blocksize = 0
        self._next_recv_macsize = 0
        self._next_decompressor: Decompressor | None = None
        self._next_decompress_after_auth = False

        self._trusted_host_keys: set[SSHKey] | None = set()
        self._trusted_host_key_algs: list[bytes] = []
        self._trusted_ca_keys: set[SSHKey] | None = set()
        self._revoked_host_keys: set[SSHKey] = set()

        self._x509_trusted_certs = options.x509_trusted_certs
        self._x509_trusted_cert_paths = options.x509_trusted_cert_paths
        self._x509_revoked_certs: set[SSHX509Certificate] = set()
        self._x509_trusted_subjects: Sequence['X509NamePattern'] = []
        self._x509_revoked_subjects: Sequence['X509NamePattern'] = []
        self._x509_purposes = options.x509_purposes

        self._kex_algs = options.kex_algs
        self._enc_algs = options.encryption_algs
        self._mac_algs = options.mac_algs
        self._cmp_algs = options.compression_algs
        self._sig_algs = options.signature_algs

        self._host_based_auth = options.host_based_auth
        self._public_key_auth = options.public_key_auth
        self._kbdint_auth = options.kbdint_auth
        self._password_auth = options.password_auth

        self._kex: Kex | None = None
        self._kexinit_sent = False
        self._kex_complete = False
        self._ignore_first_kex = False
        self._strict_kex = False

        self._gss: GSSBase | None = None
        self._gss_kex = False
        self._gss_auth = False
        self._gss_kex_auth = False
        self._gss_mic_auth = False

        self._preferred_auth: Sequence[bytes] | None = None

        self._rekey_bytes = options.rekey_bytes
        self._rekey_seconds = options.rekey_seconds
        self._rekey_bytes_sent = 0
        self._rekey_time = 0.

        self._keepalive_count = 0
        self._keepalive_count_max = options.keepalive_count_max
        self._keepalive_interval = options.keepalive_interval
        self._keepalive_timer: asyncio.TimerHandle | None = None

        self._tunnel: _TunnelProtocol | None = None

        self._enc_alg_cs = b''
        self._enc_alg_sc = b''

        self._mac_alg_cs = b''
        self._mac_alg_sc = b''

        self._cmp_alg_cs = b''
        self._cmp_alg_sc = b''

        self._can_send_ext_info = False
        self._extensions_to_send: OrderedDict[bytes, bytes] = OrderedDict()

        self._can_recv_ext_info = False

        self._server_sig_algs: set[bytes] = set()

        self._next_service: bytes | None = None

        self._agent: SSHAgentClient | None = None

        self._auth: Auth | None = None
        self._auth_in_progress = False
        self._auth_complete = False
        self._auth_final = False
        self._auth_methods = [b'none']
        self._auth_was_trivial = True
        self._username = ''

        self._channels: dict[int, SSHChannel] = {}
        self._next_recv_chan = 0

        self._global_request_queue: list[GlobalRequest] = []
        self._global_request_waiters: \
            list[asyncio.Future[GlobalRequestResult]] = []

        self._local_listeners: dict[ListenKey, SSHListener] = {}

        self._x11_listener: None | SSHX11Clientlistener | SSHX11Serverlistener = None

        self._tasks: set[asyncio.Task[None]] = set()
        self._close_event = asyncio.Event()

        self._server_host_key_algs: Sequence[bytes] |None = None

        self._login_timer: asyncio.TimerHandle | None = None

        if options.login_timeout:
            self._login_timer = self._loop.call_later(
                options.login_timeout, self._login_timer_callback)
        else:
            self._login_timer = None

        self._disable_trivial_auth = False

    async def __aenter__(self) -> Self:
        """Allow SSHConnection to be used as an async context manager"""

        return self

    async def __aexit__(self, _exc_type: Type[BaseException] | None,
                        _exc_value: BaseException | None,
                        _traceback: TracebackType | None) -> bool:
        """Wait for connection close when used as an async context manager"""

        if not self._loop.is_closed(): # pragma: no branch
            self.close()

        await self.wait_closed()
        return False


    def _cleanup(self, exc: Exception | None) -> None:
        """Clean up this connection"""

        self._cancel_keepalive_timer()

        for chan in list(self._channels.values()):
            chan.process_connection_close(exc)

        for listener in list(self._local_listeners.values()):
            listener.close()

        while self._global_request_waiters:
            self._process_global_response(MSG_REQUEST_FAILURE, 0,
                                          SSHPacket(b''))

        if self._auth:
            self._auth.cancel()
            self._auth = None

        if self._error_handler:
            self._error_handler(self, exc)
            self._acceptor = None
            self._error_handler = None

        if self._wait and self._waiter and not self._waiter.cancelled():
            if exc:
                self._waiter.set_exception(exc)
            else: # pragma: no cover
                self._waiter.set_result(None)

            self._wait = None

        if self._owner: # pragma: no branch
            # pylint: disable=broad-except
            try:
                self._owner.connection_lost(exc)
            except Exception:
                self.logger.debug1('Uncaught exception in owner ignored',
                                   exc_info=sys.exc_info)

            self._owner = None

        self._cancel_login_timer()
        self._close_event.set()

        self._inpbuf = b''

        if self._tunnel:
            self._tunnel.close()
            self._tunnel = None

    def _cancel_login_timer(self) -> None:
        """Cancel the login timer"""

        if self._login_timer:
            self._login_timer.cancel()
            self._login_timer = None

    def _login_timer_callback(self) -> None:
        """Close the connection if authentication hasn't completed yet"""

        self._login_timer = None

        self.connection_lost(ConnectionLost('Login timeout expired'))

    def _cancel_keepalive_timer(self) -> None:
        """Cancel the keepalive timer"""

        if self._keepalive_timer:
            self._keepalive_timer.cancel()
            self._keepalive_timer = None

    def _set_keepalive_timer(self) -> None:
        """set the keepalive timer"""

        if self._keepalive_interval:
            self._keepalive_timer = self._loop.call_later(
                self._keepalive_interval, self._keepalive_timer_callback)

    def _reset_keepalive_timer(self) -> None:
        """Reset the keepalive timer"""

        if self._auth_complete:
            self._cancel_keepalive_timer()
            self._set_keepalive_timer()

    async def _make_keepalive_request(self) -> None:
        """Send keepalive request"""

        self.logger.debug1('Sending keepalive request')

        await self._make_global_request(b'keepalive@openssh.com')

        if self._keepalive_timer:
            self.logger.debug1('Got keepalive response')

        self._keepalive_count = 0

    def _keepalive_timer_callback(self) -> None:
        """Handle keepalive check"""

        self._keepalive_count += 1

        if self._keepalive_count > self._keepalive_count_max:
            self.connection_lost(
                ConnectionLost(('Server' if self.is_client() else 'Client') +
                               ' not responding to keepalive'))
        else:
            self._set_keepalive_timer()
            self.create_task(self._make_keepalive_request())

    def _force_close(self, exc: Optional[Exception]) -> None:
        """Force this connection to close immediately"""

        if not self._transport:
            return

        self._loop.call_soon(self._transport.abort)
        self._transport = None

        self._loop.call_soon(self._cleanup, exc)

    def _reap_task(self, task: asyncio.Task[None]) -> None:
        """Collect result of an async task, reporting errors"""

        self._tasks.discard(task)

        # pylint: disable=broad-except
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except DisconnectError as exc:
            self._send_disconnect(exc.code, exc.reason, exc.lang)
            self._force_close(exc)
        except Exception:
            self.internal_error()

    def create_task(self, coro: Awaitable[None]) -> \
            'asyncio.Task[None]':
        """Create an asynchronous task which catches and reports errors"""

        task = asyncio.ensure_future(coro)
        task.add_done_callback(self._reap_task)
        self._tasks.add(task)

        return task

    def is_client(self) -> bool:
        """Return if this is a client connection"""

        return not self._server

    def is_closed(self) -> bool:
        """Return whether the connection is closed"""

        return self._close_event.is_set()

    def get_owner(self) -> SSHClient | None:
        """Return the SSHClient or SSHServer which owns this connection"""

        return self._owner

    def get_hash_prefix(self) -> bytes:
        """Return the bytes used in calculating unique connection hashes

           This methods returns a packetized version of the client and
           server version and kexinit strings which is needed to perform
           key exchange hashes.

        """

        return b''.join((String(self._client_version),
                         String(self._server_version),
                         String(self._client_kexinit),
                         String(self._server_kexinit)))

    def set_tunnel(self, tunnel: _TunnelProtocol) -> None:
        """set tunnel used to open this connection"""

        self._tunnel = tunnel

    def _match_known_hosts(self, known_hosts: KnownHostsArg, host: str,
                           addr: str, port: int | None) -> None:
        """Determine the set of trusted host keys and certificates"""

        trusted_host_keys, trusted_ca_keys, revoked_host_keys, \
            trusted_x509_certs, revoked_x509_certs, \
            trusted_x509_subjects, revoked_x509_subjects = \
                match_known_hosts(known_hosts, host, addr, port)

        assert self._trusted_host_keys is not None

        for key in trusted_host_keys:
            self._trusted_host_keys.add(key)

            if key.algorithm not in self._trusted_host_key_algs:
                self._trusted_host_key_algs.extend(key.sig_algorithms)

        self._trusted_ca_keys = set(trusted_ca_keys)
        self._revoked_host_keys = set(revoked_host_keys)

        if self._x509_trusted_certs is not None:
            self._x509_trusted_certs = list(self._x509_trusted_certs)
            self._x509_trusted_certs.extend(trusted_x509_certs)
            self._x509_revoked_certs = set(revoked_x509_certs)

            self._x509_trusted_subjects = trusted_x509_subjects
            self._x509_revoked_subjects = revoked_x509_subjects

    def _validate_openssh_host_certificate(
            self, host: str, addr: str, port: int,
            cert: SSHOpenSSHCertificate) -> SSHKey:
        """Validate an OpenSSH host certificate"""

        if self._trusted_ca_keys is not None:
            if cert.signing_key in self._revoked_host_keys:
                raise ValueError('Host CA key is revoked')

            if not self._owner: # pragma: no cover
                raise ValueError('Connection closed')

            if cert.signing_key not in self._trusted_ca_keys and \
               not self._owner.validate_host_ca_key(host, addr, port,
                                                    cert.signing_key):
                raise ValueError('Host CA key is not trusted')

            cert.validate(CERT_TYPE_HOST, host)

        return cert.key

    def _validate_x509_host_certificate_chain(
            self, host: str, cert: SSHX509CertificateChain) -> SSHKey:
        """Validate an X.509 host certificate"""

        if (self._x509_revoked_subjects and
                any(pattern.matches(cert.subject)
                    for pattern in self._x509_revoked_subjects)):
            raise ValueError('X.509 subject name is revoked')

        if (self._x509_trusted_subjects and
                not any(pattern.matches(cert.subject)
                        for pattern in self._x509_trusted_subjects)):
            raise ValueError('X.509 subject name is not trusted')

        # Only validate hostname against X.509 certificate host
        # principals when there are no X.509 trusted subject
        # entries matched in known_hosts.
        if self._x509_trusted_subjects:
            host = ''

        assert self._x509_trusted_certs is not None

        cert.validate_chain(self._x509_trusted_certs,
                            self._x509_trusted_cert_paths,
                            self._x509_revoked_certs,
                            self._x509_purposes,
                            host_principal=host)

        return cert.key

    def _validate_host_key(self, host: str, addr: str, port: int,
                           key_data: bytes) -> SSHKey:
        """Validate and return a trusted host key"""

        try:
            cert = decode_ssh_certificate(key_data)
        except KeyImportError:
            pass
        else:
            if cert.is_x509_chain:
                return self._validate_x509_host_certificate_chain(
                    host, cert)
            else:
                return self._validate_openssh_host_certificate(
                    host, addr, port, cert)

        try:
            key = decode_ssh_public_key(key_data)
        except KeyImportError:
            pass
        else:
            if self._trusted_host_keys is not None:
                if key in self._revoked_host_keys:
                    raise ValueError('Host key is revoked')

                if not self._owner: # pragma: no cover
                    raise ValueError('Connection closed')

                if key not in self._trusted_host_keys and \
                   not self._owner.validate_host_public_key(host, addr,
                                                            port, key):
                    raise ValueError('Host key is not trusted')

            return key

        raise ValueError('Unable to decode host key')

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Handle a newly opened connection"""

        self._transport = cast(asyncio.Transport, transport)

        sock = cast(socket.socket, transport.get_extra_info('socket'))

        if sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE,
                            1 if self._tcp_keepalive else 0)

            if sock.family in (socket.AF_INET, socket.AF_INET6):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        sockname = cast(SockAddr, transport.get_extra_info('sockname'))

        if sockname: # pragma: no branch
            self._local_addr, self._local_port = sockname[:2]

        peername = cast(SockAddr, transport.get_extra_info('peername'))

        if peername: # pragma: no branch
            self._peer_addr, self._peer_port = peername[:2]

        self._owner = self._protocol_factory()

        # pylint: disable=broad-except
        try:
            self._connection_made()
            self._owner.connection_made(self) # type: ignore
            self._send_version()
        except Exception:
            self._loop.call_soon(self.internal_error, sys.exc_info())

    def connection_lost(self, exc: Exception | None = None) -> None:
        """Handle the closing of a connection"""

        if exc is None and self._transport:
            exc = ConnectionLost('Connection lost')

        self._force_close(exc)

    def internal_error(self, exc_info: OptExcInfo | None = None) -> None:
        """Handle a fatal error in connection processing"""

        if not exc_info:
            exc_info = sys.exc_info()

        self._force_close(cast(Exception, exc_info[1]))

    def session_started(self) -> None:
        """Handle session start when opening tunneled SSH connection"""

    # pylint: disable=arguments-differ
    def data_received(self, data: bytes, datatype: DataType = None) -> None:
        """Handle incoming data on the connection"""

        # pylint: disable=unused-argument

        self._inpbuf += data

        self._recv_data()
    # pylint: enable=arguments-differ

    def eof_received(self) -> None:
        """Handle an incoming end of file on the connection"""

        self.connection_lost(None)

    def pause_writing(self) -> None:
        """Handle a request from the transport to pause writing data"""

        # Do nothing with this for now

    def resume_writing(self) -> None:
        """Handle a request from the transport to resume writing data"""

        # Do nothing with this for now

    def add_channel(self, chan: SSHChannel[AnyStr]) -> int:
        """Add a new channel, returning its channel number"""

        if not self._transport:
            raise ChannelOpenError(OPEN_CONNECT_FAILED,
                                   'SSH connection closed')

        while self._next_recv_chan in self._channels: # pragma: no cover
            self._next_recv_chan = (self._next_recv_chan + 1) & 0xffffffff

        recv_chan = self._next_recv_chan
        self._next_recv_chan = (self._next_recv_chan + 1) & 0xffffffff

        self._channels[recv_chan] = chan
        return recv_chan

    def remove_channel(self, recv_chan: int) -> None:
        """Remove the channel with the specified channel number"""

        del self._channels[recv_chan]

    def get_gss_context(self) -> GSSBase:
        """Return the GSS context associated with this connection"""

        assert self._gss is not None
        return self._gss

    def enable_gss_kex_auth(self) -> None:
        """Enable GSS key exchange authentication"""

        self._gss_kex_auth = self._gss_auth

    def _choose_alg(self, alg_type: str, local_algs: Sequence[bytes],
                    remote_algs: Sequence[bytes]) -> bytes:
        """Choose a common algorithm from the client & server lists

           This method returns the earliest algorithm on the client's
           list which is supported by the server.

        """

        if self.is_client():
            client_algs, server_algs = local_algs, remote_algs
        else:
            client_algs, server_algs = remote_algs, local_algs

        for alg in client_algs:
            if alg in server_algs:
                return alg

        raise KeyExchangeFailed(
            f'No matching {alg_type} algorithm found, sent '
            f'{b",".join(local_algs).decode("ascii")} and received '
            f'{b",".join(remote_algs).decode("ascii")}')

    def _get_extra_kex_algs(self) -> list[bytes]:
        """Return the extra kex algs to add"""

        if self.is_client():
            return [b'ext-info-c', b'kex-strict-c-v00@openssh.com']
        else:
            return [b'ext-info-s', b'kex-strict-s-v00@openssh.com']

    def _send(self, data: bytes) -> None:
        """Send data to the SSH connection"""

        if self._transport:
            try:
                self._transport.write(data)
            except ConnectionError: # pragma: no cover
                pass

    def _send_version(self) -> None:
        """Start the SSH handshake"""

        version = b'SSH-2.0-' + self._version
        if self.is_client():
            self._client_version = version
            self.set_extra_info(client_version=version.decode('ascii'))
        else:
            self._server_version = version
            self.set_extra_info(server_version=version.decode('ascii'))

        self._send(version + b'\r\n')

    def _recv_data(self) -> None:
        """Parse received data"""

        self._reset_keepalive_timer()

        # pylint: disable=broad-except
        try:
            while self._inpbuf and self._recv_handler():
                pass
        except DisconnectError as exc:
            self._send_disconnect(exc.code, exc.reason, exc.lang)
            self._force_close(exc)
        except Exception:
            self.internal_error()

    def _recv_version(self) -> bool:
        """Receive and parse the remote SSH version"""

        idx = self._inpbuf.find(b'\n', 0, MAX_BANNER_LINE_LEN)
        if idx < 0:
            if len(self._inpbuf) >= MAX_BANNER_LINE_LEN:
                self._force_close(ProtocolError('Banner line too long'))

            return False

        version = self._inpbuf[:idx]
        if version.endswith(b'\r'):
            version = version[:-1]

        self._inpbuf = self._inpbuf[idx+1:]

        if (version.startswith(b'SSH-2.0-') or
                (self.is_client() and version.startswith(b'SSH-1.99-'))):
            if len(version) > MAX_VERSION_LINE_LEN:
                self._force_close(ProtocolError('Version too long'))


            self._server_version = version
            self.set_extra_info(server_version=version.decode('ascii'))

            self._send_kexinit()
            self._kexinit_sent = True
            self._recv_handler = self._recv_pkthdr
        elif self.is_client() and not version.startswith(b'SSH-'):
            # As a client, ignore the line if it doesn't appear to be a version
            self._banner_lines += 1

            if self._banner_lines > MAX_BANNER_LINES:
                self._force_close(ProtocolError('Too many banner lines'))
                return False
        else:
            # Otherwise, reject the unknown version
            self._force_close(ProtocolNotSupported('Unsupported SSH version'))
            return False

        return True

    def _recv_pkthdr(self) -> bool:
        """Receive and parse an SSH packet header"""

        if len(self._inpbuf) < self._recv_blocksize:
            return False

        self._packet = self._inpbuf[:self._recv_blocksize]
        self._inpbuf = self._inpbuf[self._recv_blocksize:]

        if self._recv_encryption:
            self._packet, pktlen = \
                self._recv_encryption.decrypt_header(self._recv_seq,
                                                     self._packet, 4)
        else:
            pktlen = self._packet[:4]

        self._pktlen = int.from_bytes(pktlen, 'big')
        self._recv_handler = self._recv_packet
        return True

    def _recv_packet(self) -> bool:
        """Receive the remainder of an SSH packet and process it"""

        rem = 4 + self._pktlen + self._recv_macsize - self._recv_blocksize
        if len(self._inpbuf) < rem:
            return False

        seq = self._recv_seq
        rest = self._inpbuf[:rem-self._recv_macsize]
        mac = self._inpbuf[rem-self._recv_macsize:rem]

        if self._recv_encryption:
            packet_data = self._recv_encryption.decrypt_packet(
                seq, self._packet, rest, 4, mac)

            if not packet_data:
                raise MACError('MAC verification failed')
        else:
            packet_data = self._packet[4:] + rest

        self._inpbuf = self._inpbuf[rem:]
        self._packet = b''

        orig_payload = packet_data[1:-packet_data[0]]

        if self._decompressor and (self._auth_complete or
                                   not self._decompress_after_auth):
            payload = self._decompressor.decompress(orig_payload)

            if payload is None:
                raise CompressionError('Decompression failed')
        else:
            payload = orig_payload

        packet = SSHPacket(payload)
        pkttype = packet.get_byte()
        handler: SSHPacketHandler = self
        skip_reason = ''
        exc_reason = ''

        if MSG_KEX_FIRST <= pkttype <= MSG_KEX_LAST:
            if self._kex:
                if self._ignore_first_kex: # pragma: no cover
                    skip_reason = 'ignored first kex'
                    self._ignore_first_kex = False
                else:
                    handler = self._kex
            else:
                skip_reason = 'kex not in progress'
                exc_reason = 'Key exchange not in progress'
        elif self._strict_kex and not self._recv_encryption and \
                MSG_IGNORE <= pkttype <= MSG_DEBUG:
            skip_reason = 'strict kex violation'
            exc_reason = 'Strict key exchange violation: ' \
                         f'unexpected packet type {pkttype} received'
        elif MSG_USERAUTH_FIRST <= pkttype <= MSG_USERAUTH_LAST:
            if self._auth:
                handler = self._auth
            else:
                skip_reason = 'auth not in progress'
                exc_reason = 'Authentication not in progress'
        elif pkttype > MSG_KEX_LAST and not self._recv_encryption:
            skip_reason = 'invalid request before kex complete'
            exc_reason = 'Invalid request before key exchange was complete'
        elif pkttype > MSG_USERAUTH_LAST and not self._auth_complete:
            skip_reason = 'invalid request before auth complete'
            exc_reason = 'Invalid request before authentication was complete'
        elif MSG_CHANNEL_FIRST <= pkttype <= MSG_CHANNEL_LAST:
            try:
                recv_chan = packet.get_uint32()
            except PacketDecodeError:
                skip_reason = 'incomplete channel request'
                exc_reason = 'Incomplete channel request received'
            else:
                try:
                    handler = self._channels[recv_chan]
                except KeyError:
                    skip_reason = 'invalid channel number'
                    exc_reason = f'Invalid channel number {recv_chan} received'

        handler.log_received_packet(pkttype, seq, packet, skip_reason)

        if not skip_reason:
            try:
                result = handler.process_packet(pkttype, seq, packet)
            except PacketDecodeError as exc:
                raise ProtocolError(str(exc)) from None

            if inspect.isawaitable(result):
                # Buffer received data until current packet is processed
                self._recv_handler = lambda: False

                task = self.create_task(result)
                task.add_done_callback(functools.partial(
                    self._finish_recv_packet, pkttype, seq, is_async=True))

                return False
            elif not result:
                if self._strict_kex and not self._recv_encryption:
                    exc_reason = 'Strict key exchange violation: ' \
                                 f'unexpected packet type {pkttype} received'
                else:
                    self.send_packet(MSG_UNIMPLEMENTED, UInt32(seq))

        if exc_reason:
            raise ProtocolError(exc_reason)

        self._finish_recv_packet(pkttype, seq)
        return True

    def _finish_recv_packet(self, pkttype: int, seq: int,
                            _task: asyncio.Task | None = None,
                            is_async: bool = False) -> None:
        """Finish processing a packet"""

        if pkttype > MSG_USERAUTH_LAST:
            self._auth_final = True

        if self._transport:
            if self._recv_seq == 0xffffffff and not self._recv_encryption:
                raise ProtocolError('Sequence rollover before kex complete')

            if pkttype == MSG_NEWKEYS and self._strict_kex:
                self._recv_seq = 0
            else:
                self._recv_seq = (seq + 1) & 0xffffffff

        self._recv_handler = self._recv_pkthdr

        if is_async and self._inpbuf:
            self._recv_data()

    def send_packet(self, pkttype: int, *args: bytes,
                    handler: SSHPacketLogger | None = None) -> None:
        """Send an SSH packet"""

        if (self._auth_complete and self._kex_complete and
                (self._rekey_bytes_sent >= self._rekey_bytes or
                 (self._rekey_seconds and
                  time.monotonic() >= self._rekey_time))):
            self._send_kexinit()
            self._kexinit_sent = True

        if (((pkttype in {MSG_DEBUG, MSG_SERVICE_REQUEST, MSG_SERVICE_ACCEPT} or
              pkttype > MSG_KEX_LAST) and not self._kex_complete) or
                (pkttype == MSG_USERAUTH_BANNER and
                 not (self._auth_in_progress or self._auth_complete)) or
                (pkttype > MSG_USERAUTH_LAST and not self._auth_complete)):
            self._deferred_packets.append((pkttype, args))
            return

        # If we're encrypting and we have no data outstanding, insert an
        # ignore packet into the stream
        if self._send_encryption and pkttype > MSG_KEX_LAST:
            self.send_packet(MSG_IGNORE, String(b''))

        orig_payload = Byte(pkttype) + b''.join(args)

        if self._compressor and (self._auth_complete or
                                 not self._compress_after_auth):
            payload = self._compressor.compress(orig_payload)

            if payload is None: # pragma: no cover
                raise CompressionError('Compression failed')
        else:
            payload = orig_payload

        padlen = -(self._send_enchdrlen + len(payload)) % self._send_blocksize
        if padlen < 4:
            padlen += self._send_blocksize

        packet = Byte(padlen) + payload + os.urandom(padlen)
        pktlen = len(packet)
        hdr = UInt32(pktlen)
        seq = self._send_seq

        if self._send_encryption:
            packet, mac = self._send_encryption.encrypt_packet(seq, hdr, packet)
        else:
            packet = hdr + packet
            mac = b''

        self._send(packet + mac)

        if self._send_seq == 0xffffffff and not self._send_encryption:
            self._send_seq = 0
            raise ProtocolError('Sequence rollover before kex complete')

        if pkttype == MSG_NEWKEYS and self._strict_kex:
            self._send_seq = 0
        else:
            self._send_seq = (seq + 1) & 0xffffffff

        if self._kex_complete:
            self._rekey_bytes_sent += pktlen

        if not handler:
            handler = self

        handler.log_sent_packet(pkttype, seq, orig_payload)

    def _send_deferred_packets(self) -> None:
        """Send packets deferred due to key exchange or auth"""

        deferred_packets = self._deferred_packets
        self._deferred_packets = []

        for pkttype, args in deferred_packets:
            self.send_packet(pkttype, *args)

    def _send_disconnect(self, code: int, reason: str, lang: str) -> None:
        """Send a disconnect packet"""
        self.send_packet(MSG_DISCONNECT, UInt32(code),
                         String(reason), String(lang))

    def _send_kexinit(self) -> None:
        """Start a key exchange"""

        self._kex_complete = False
        self._rekey_bytes_sent = 0

        if self._rekey_seconds:
            self._rekey_time = time.monotonic() + self._rekey_seconds

        if self._gss_kex:
            assert self._gss is not None
            gss_mechs = self._gss.mechs
        else:
            gss_mechs = []

        kex_algs = expand_kex_algs(self._kex_algs, gss_mechs,
                                   bool(self._server_host_key_algs)) + \
                   self._get_extra_kex_algs()

        host_key_algs = self._server_host_key_algs or [b'null']

        cookie = os.urandom(16)
        kex_algs = NameList(kex_algs)
        host_key_algs = NameList(host_key_algs)
        enc_algs = NameList(self._enc_algs)
        mac_algs = NameList(self._mac_algs)
        cmp_algs = NameList(self._cmp_algs)
        langs = NameList([])

        packet = b''.join((Byte(MSG_KEXINIT), cookie, kex_algs, host_key_algs,
                           enc_algs, enc_algs, mac_algs, mac_algs, cmp_algs,
                           cmp_algs, langs, langs, Boolean(False), UInt32(0)))

        self._client_kexinit = packet

        self.send_packet(MSG_KEXINIT, packet[1:])

    def _send_ext_info(self) -> None:
        """Send extension information"""

        packet = UInt32(len(self._extensions_to_send))


        for name, value in self._extensions_to_send.items():
            packet += String(name) + String(value)

        self.send_packet(MSG_EXT_INFO, packet)

    def send_newkeys(self, k: bytes, h: bytes) -> None:
        """Finish a key exchange and send a new keys message"""

        if not self._session_id:
            first_kex = True
            self._session_id = h
        else:
            first_kex = False

        enc_keysize_cs, enc_ivsize_cs, enc_blocksize_cs, \
        mac_keysize_cs, mac_hashsize_cs, etm_cs = \
            get_encryption_params(self._enc_alg_cs, self._mac_alg_cs)

        enc_keysize_sc, enc_ivsize_sc, enc_blocksize_sc, \
        mac_keysize_sc, mac_hashsize_sc, etm_sc = \
            get_encryption_params(self._enc_alg_sc, self._mac_alg_sc)

        if mac_keysize_cs == 0:
            self._mac_alg_cs = self._enc_alg_cs

        if mac_keysize_sc == 0:
            self._mac_alg_sc = self._enc_alg_sc

        cmp_after_auth_cs = get_compression_params(self._cmp_alg_cs)
        cmp_after_auth_sc = get_compression_params(self._cmp_alg_sc)

        assert self._kex is not None

        iv_cs = self._kex.compute_key(k, h, b'A', self._session_id,
                                      enc_ivsize_cs)
        iv_sc = self._kex.compute_key(k, h, b'B', self._session_id,
                                      enc_ivsize_sc)
        enc_key_cs = self._kex.compute_key(k, h, b'C', self._session_id,
                                           enc_keysize_cs)
        enc_key_sc = self._kex.compute_key(k, h, b'D', self._session_id,
                                           enc_keysize_sc)
        mac_key_cs = self._kex.compute_key(k, h, b'E', self._session_id,
                                           mac_keysize_cs)
        mac_key_sc = self._kex.compute_key(k, h, b'F', self._session_id,
                                           mac_keysize_sc)
        self._kex = None

        next_enc_cs = get_encryption(self._enc_alg_cs, enc_key_cs, iv_cs,
                                     self._mac_alg_cs, mac_key_cs, etm_cs)
        next_enc_sc = get_encryption(self._enc_alg_sc, enc_key_sc, iv_sc,
                                     self._mac_alg_sc, mac_key_sc, etm_sc)

        self.send_packet(MSG_NEWKEYS)

        self._extensions_to_send[b'global-requests-ok'] = b''

        if self.is_client():
            self._send_encryption = next_enc_cs
            self._send_enchdrlen = 1 if etm_cs else 5
            self._send_blocksize = max(8, enc_blocksize_cs)
            self._compressor = get_compressor(self._cmp_alg_cs)
            self._compress_after_auth = cmp_after_auth_cs

            self._next_recv_encryption = next_enc_sc
            self._next_recv_blocksize = max(8, enc_blocksize_sc)
            self._next_recv_macsize = mac_hashsize_sc
            self._next_decompressor = get_decompressor(self._cmp_alg_sc)
            self._next_decompress_after_auth = cmp_after_auth_sc

            self.set_extra_info(
                send_cipher=self._enc_alg_cs.decode('ascii'),
                send_mac=self._mac_alg_cs.decode('ascii'),
                send_compression=self._cmp_alg_cs.decode('ascii'),
                recv_cipher=self._enc_alg_sc.decode('ascii'),
                recv_mac=self._mac_alg_sc.decode('ascii'),
                recv_compression=self._cmp_alg_sc.decode('ascii'))

            if first_kex:
                if self._wait == 'kex' and self._waiter and \
                        not self._waiter.cancelled():
                    self._waiter.set_result(None)
                    self._wait = None
                    return
        else:
            self._extensions_to_send[b'server-sig-algs'] = \
                b','.join(self._sig_algs)

            self._send_encryption = next_enc_sc
            self._send_enchdrlen = 1 if etm_sc else 5
            self._send_blocksize = max(8, enc_blocksize_sc)
            self._compressor = get_compressor(self._cmp_alg_sc)
            self._compress_after_auth = cmp_after_auth_sc

            self._next_recv_encryption = next_enc_cs
            self._next_recv_blocksize = max(8, enc_blocksize_cs)
            self._next_recv_macsize = mac_hashsize_cs
            self._next_decompressor = get_decompressor(self._cmp_alg_cs)
            self._next_decompress_after_auth = cmp_after_auth_cs

            self.set_extra_info(
                send_cipher=self._enc_alg_sc.decode('ascii'),
                send_mac=self._mac_alg_sc.decode('ascii'),
                send_compression=self._cmp_alg_sc.decode('ascii'),
                recv_cipher=self._enc_alg_cs.decode('ascii'),
                recv_mac=self._mac_alg_cs.decode('ascii'),
                recv_compression=self._cmp_alg_cs.decode('ascii'))

        if self._can_send_ext_info:
            self._send_ext_info()
            self._can_send_ext_info = False

        self._kex_complete = True

        if first_kex:
            if self.is_client():
                self.send_service_request(USERAUTH_SERVICE)
            else:
                self._next_service = USERAUTH_SERVICE

        self._send_deferred_packets()

    def send_service_request(self, service: bytes) -> None:
        """Send a service request"""

        self.logger.debug2('Requesting service %s', service)

        self._next_service = service
        self.send_packet(MSG_SERVICE_REQUEST, String(service))

    def _get_userauth_request_packet(self, method: bytes,
                                     args: tuple[bytes, ...]) -> bytes:
        """Get packet data for a user authentication request"""

        return b''.join((Byte(MSG_USERAUTH_REQUEST), String(self._username),
                         String(CONNECTION_SERVICE), String(method)) + args)

    def get_userauth_request_data(self, method: bytes, *args: bytes) -> bytes:
        """Get signature data for a user authentication request"""

        return (String(self._session_id) +
                self._get_userauth_request_packet(method, args))

    def send_userauth_packet(self, pkttype: int, *args: bytes,
                             handler: Any | None = None,
                             trivial: bool = True) -> None:
        """Send a user authentication packet"""

        self._auth_was_trivial &= trivial
        self.send_packet(pkttype, *args, handler=handler)

    async def send_userauth_request(self, method: bytes, *args: bytes,
                                    key: SigningKey | None = None,
                                    trivial: bool = True) -> None:
        """Send a user authentication request"""

        packet = self._get_userauth_request_packet(method, args)

        if key:
            data = String(self._session_id) + packet

            sign_async: Callable[[bytes], Awaitable[bytes]] | None = \
                getattr(key, 'sign_async', None)

            if sign_async:
                # pylint: disable=not-callable
                sig = await sign_async(data)
            elif getattr(key, 'use_executor', False):
                sig = await self._loop.run_in_executor(None, key.sign, data)
            else:
                sig = key.sign(data)

            packet += String(sig)

        self.send_userauth_packet(MSG_USERAUTH_REQUEST, packet[1:],
                                  trivial=trivial)

    def send_userauth_failure(self, partial_success: bool) -> None:
        """Send a user authentication failure response"""

        methods = get_supported_server_auth_methods(self)

        self._auth = None
        self.send_packet(MSG_USERAUTH_FAILURE, NameList(methods),
                         Boolean(partial_success))

    async def send_userauth_success(self) -> None:
        """Send a user authentication success response"""

        self.logger.info('Auth for user %s succeeded', self._username)

        self.send_packet(MSG_USERAUTH_SUCCESS)
        self._auth = None
        self._auth_in_progress = False
        self._auth_complete = True
        self._next_service = None
        self.set_extra_info(username=self._username)
        self._send_deferred_packets()

        self._cancel_login_timer()
        self._set_keepalive_timer()

        if self._owner: # pragma: no branch
            result = self._owner.auth_completed()

            if inspect.isawaitable(result):
                await result

        if self._acceptor:
            result = self._acceptor(self)

            if inspect.isawaitable(result):
                self.create_task(result)

            self._acceptor = None
            self._error_handler = None

        if self._wait == 'auth' and self._waiter and \
                not self._waiter.cancelled():
            self._waiter.set_result(None)
            self._wait = None
            return

    def send_channel_open_confirmation(self, send_chan: int, recv_chan: int,
                                       recv_window: int, recv_pktsize: int,
                                       *result_args: bytes) -> None:
        """Send a channel open confirmation"""

        self.send_packet(MSG_CHANNEL_OPEN_CONFIRMATION, UInt32(send_chan),
                         UInt32(recv_chan), UInt32(recv_window),
                         UInt32(recv_pktsize), *result_args)

    def send_channel_open_failure(self, send_chan: int, code: int,
                                  reason: str, lang: str) -> None:
        """Send a channel open failure"""

        self.send_packet(MSG_CHANNEL_OPEN_FAILURE, UInt32(send_chan),
                         UInt32(code), String(reason), String(lang))

    def _send_global_request(self, request: bytes, *args: bytes,
                             want_reply: bool = False) -> None:
        """Send a global request"""

        self.send_packet(MSG_GLOBAL_REQUEST, String(request),
                         Boolean(want_reply), *args)

    async def _make_global_request(self, request: bytes,
                                   *args: bytes) -> tuple[int, SSHPacket]:
        """Send a global request and wait for the response"""

        if not self._transport:
            return MSG_REQUEST_FAILURE, SSHPacket(b'')

        waiter: asyncio.Future[GlobalRequestResult] = \
            self._loop.create_future()

        self._global_request_waiters.append(waiter)

        self._send_global_request(request, *args, want_reply=True)

        return await waiter

    def _report_global_response(self, result: bool | bytes) -> None:
        """Report back the response to a previously issued global request"""

        _, _, want_reply = self._global_request_queue.pop(0)

        if want_reply: # pragma: no branch
            if result:
                response = b'' if result is True else cast(bytes, result)
                self.send_packet(MSG_REQUEST_SUCCESS, response)
            else:
                self.send_packet(MSG_REQUEST_FAILURE)

        if self._global_request_queue:
            self._service_next_global_request()

    def _service_next_global_request(self) -> None:
        """Process next item on global request queue"""

        handler, packet, _ = self._global_request_queue[0]
        if callable(handler):
            handler(packet)
        else:
            self._report_global_response(False)

    def _connection_made(self) -> None:
        """Handle the opening of a new connection"""

        raise NotImplementedError

    def _process_disconnect(self, _pkttype: int, _pktid: int,
                            packet: SSHPacket) -> None:
        """Process a disconnect message"""

        code = packet.get_uint32()
        reason_bytes = packet.get_string()
        lang_bytes = packet.get_string()
        packet.check_end()

        try:
            reason = reason_bytes.decode('utf-8')
            lang = lang_bytes.decode('ascii')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid disconnect message') from None

        if code != DISC_BY_APPLICATION or self._wait:
            exc: Exception | None = construct_disc_error(code, reason, lang)
        else:
            exc = None

        self._force_close(exc)

    def _process_ignore(self, _pkttype: int, _pktid: int,
                        packet: SSHPacket) -> None:
        """Process an ignore message"""

        # Work around missing payload bytes in an ignore message
        # in some Cisco SSH servers
        if b'Cisco' not in self._server_version: # pragma: no branch
            _ = packet.get_string()     # data
            packet.check_end()

    def _process_unimplemented(self, _pkttype: int, _pktid: int,
                               packet: SSHPacket) -> None:
        """Process an unimplemented message response"""

        # pylint: disable=no-self-use

        _ = packet.get_uint32()     # seq
        packet.check_end()

    def _process_debug(self, _pkttype: int, _pktid: int,
                       packet: SSHPacket) -> None:
        """Process a debug message"""

        always_display = packet.get_boolean()
        msg_bytes = packet.get_string()
        lang_bytes = packet.get_string()
        packet.check_end()

        try:
            msg = msg_bytes.decode('utf-8')
            lang = lang_bytes.decode('ascii')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid debug message') from None

        self.logger.debug1('Received debug message: %s%s', msg,
                           ' (always display)' if always_display else '')

        if self._owner: # pragma: no branch
            self._owner.debug_msg_received(msg, lang, always_display)

    def _process_service_request(self, _pkttype: int, _pktid: int,
                                 packet: SSHPacket) -> None:
        """Process a service request"""

        service = packet.get_string()
        packet.check_end()

        if self.is_client():
            raise ProtocolError('Unexpected service request received')

        if not self._recv_encryption:
            raise ProtocolError('Service request received before kex complete')

        if service != self._next_service:
            raise ServiceNotAvailable('Unexpected service in service request')

        self.logger.debug2('Accepting request for service %s', service)

        self.send_packet(MSG_SERVICE_ACCEPT, String(service))

        self._next_service = None

        if service == USERAUTH_SERVICE: # pragma: no branch
            self._auth_in_progress = True
            self._can_recv_ext_info = False
            self._send_deferred_packets()

    def _process_service_accept(self, _pkttype: int, _pktid: int,
                                packet: SSHPacket) -> None:
        """Process a service accept response"""

        service = packet.get_string()
        packet.check_end()

        if not self._recv_encryption:
            raise ProtocolError('Service accept received before kex complete')

        if service != self._next_service:
            raise ServiceNotAvailable('Unexpected service in service accept')

        self._next_service = None

        if service == USERAUTH_SERVICE: # pragma: no branch

            self._auth_in_progress = True

            if self._owner: # pragma: no branch
                self._owner.begin_auth(self._username)

            # This method is only in SSHClientConnection
            # pylint: disable=no-member
            self.try_next_auth()

    def _process_ext_info(self, _pkttype: int, _pktid: int,
                          packet: SSHPacket) -> None:
        """Process extension information"""

        if not self._can_recv_ext_info:
            raise ProtocolError('Unexpected ext_info received')

        extensions: dict[bytes, bytes] = {}
        num_extensions = packet.get_uint32()
        for _ in range(num_extensions):
            name = packet.get_string()
            value = packet.get_string()
            extensions[name] = value

        packet.check_end()

        if self.is_client():
            self._server_sig_algs = \
                set(extensions.get(b'server-sig-algs', b'').split(b','))

    async def _process_kexinit(self, _pkttype: int, _pktid: int,
                               packet: SSHPacket) -> None:
        """Process a key exchange request"""

        if self._kex:
            raise ProtocolError('Key exchange already in progress')

        _ = packet.get_bytes(16)                        # cookie
        peer_kex_algs = packet.get_namelist()
        peer_host_key_algs = packet.get_namelist()
        enc_algs_cs = packet.get_namelist()
        enc_algs_sc = packet.get_namelist()
        mac_algs_cs = packet.get_namelist()
        mac_algs_sc = packet.get_namelist()
        cmp_algs_cs = packet.get_namelist()
        cmp_algs_sc = packet.get_namelist()
        _ = packet.get_namelist()                       # lang_cs
        _ = packet.get_namelist()                       # lang_sc
        first_kex_follows = packet.get_boolean()
        _ = packet.get_uint32()                         # reserved
        packet.check_end()


        self._server_kexinit = packet.get_consumed_payload()

        if not self._session_id:
            if b'ext-info-s' in peer_kex_algs:
                self._can_send_ext_info = True

            if b'kex-strict-s-v00@openssh.com' in peer_kex_algs:
                self._strict_kex = True

        if self._strict_kex and not self._recv_encryption and \
                self._recv_seq != 0:
            raise ProtocolError('Strict key exchange violation: '
                                'KEXINIT was not the first packet')


        if self._kexinit_sent:
            self._kexinit_sent = False
        else:
            self._send_kexinit()

        if self._gss:
            self._gss.reset()

        if self._gss_kex:
            assert self._gss is not None
            gss_mechs = self._gss.mechs
        else:
            gss_mechs = []

        kex_algs = expand_kex_algs(self._kex_algs, gss_mechs,
                                   bool(self._server_host_key_algs))


        kex_alg = self._choose_alg('key exchange', kex_algs, peer_kex_algs)
        self._kex = get_kex(self, kex_alg)
        self._ignore_first_kex = (first_kex_follows and
                                  self._kex.algorithm != peer_kex_algs[0])

        self._enc_alg_cs = self._choose_alg('encryption', self._enc_algs,
                                            enc_algs_cs)
        self._enc_alg_sc = self._choose_alg('encryption', self._enc_algs,
                                            enc_algs_sc)

        self._mac_alg_cs = self._choose_alg('MAC', self._mac_algs, mac_algs_cs)
        self._mac_alg_sc = self._choose_alg('MAC', self._mac_algs, mac_algs_sc)

        self._cmp_alg_cs = self._choose_alg('compression', self._cmp_algs,
                                            cmp_algs_cs)
        self._cmp_alg_sc = self._choose_alg('compression', self._cmp_algs,
                                            cmp_algs_sc)


        await self._kex.start()

    def _process_newkeys(self, _pkttype: int, _pktid: int,
                         packet: SSHPacket) -> None:
        """Process a new keys message, finishing a key exchange"""

        packet.check_end()

        if self._next_recv_encryption:
            self._recv_encryption = self._next_recv_encryption
            self._recv_blocksize = self._next_recv_blocksize
            self._recv_macsize = self._next_recv_macsize
            self._decompressor = self._next_decompressor
            self._decompress_after_auth = self._next_decompress_after_auth

            self._next_recv_encryption = None
            self._can_recv_ext_info = True
        else:
            raise ProtocolError('New keys not negotiated')

    def _process_userauth_request(self, _pkttype: int, _pktid: int,
                                  packet: SSHPacket) -> None:
        """Process a user authentication request"""

        username_bytes = packet.get_string()
        service = packet.get_string()
        method = packet.get_string()

        if len(username_bytes) >= MAX_USERNAME_LEN:
            raise IllegalUserName('Username too long')

        if service != CONNECTION_SERVICE:
            raise ServiceNotAvailable('Unexpected service in auth request')

        try:
            username = saslprep(username_bytes.decode('utf-8'))
        except (UnicodeDecodeError, SASLPrepError) as exc:
            raise IllegalUserName(str(exc)) from None

        if self.is_client():
            raise ProtocolError('Unexpected userauth request')
        elif self._auth_complete:
            # Silently ignore additional auth requests after auth succeeds,
            # until the client sends a non-auth message
            if self._auth_final:
                raise ProtocolError('Unexpected userauth request')
        else:
            if username != self._username:
                self.logger.info('Beginning auth for user %s', username)

                self._username = username
                begin_auth = True
            else:
                begin_auth = False

            self.create_task(self._finish_userauth(begin_auth, method, packet))

    def _process_userauth_failure(self, _pkttype: int, _pktid: int,
                                  packet: SSHPacket) -> None:
        """Process a user authentication failure response"""

        auth_methods = packet.get_namelist()
        partial_success = packet.get_boolean()
        packet.check_end()

        if self._wait == 'auth_methods' and self._waiter and \
                not self._waiter.cancelled():
            self._waiter.set_result(None)
            self._auth_methods = list(auth_methods)
            self._wait = None
            return

        if self._preferred_auth:
            auth_methods = [method for method in self._preferred_auth
                            if method in auth_methods]

        self._auth_methods = list(auth_methods)

        if self.is_client() and self._auth:
            auth: ClientAuth = self._auth

            if partial_success: # pragma: no cover
                # Partial success not implemented yet
                auth.auth_succeeded()
            else:
                auth.auth_failed()

            # This method is only in SSHClientConnection
            # pylint: disable=no-member
            cast(SSHClientConnection, self).try_next_auth()
        else:
            raise ProtocolError('Unexpected userauth failure response')

    def _process_userauth_success(self, _pkttype: int, _pktid: int,
                                  packet: SSHPacket) -> None:
        """Process a user authentication success response"""

        packet.check_end()

        if self.is_client() and self._auth:
            auth: ClientAuth = self._auth

            if self._auth_was_trivial and self._disable_trivial_auth:
                raise PermissionDenied('Trivial auth disabled')

            if self._wait == 'auth_methods' and self._waiter and \
                    not self._waiter.cancelled():
                self._waiter.set_result(None)
                self._auth_methods = [b'none']
                self._wait = None
                return

            auth.auth_succeeded()
            auth.cancel()
            self._auth = None
            self._auth_in_progress = False
            self._auth_complete = True
            self._can_recv_ext_info = False

            if self._agent:
                self._agent.close()

            self.set_extra_info(username=self._username)
            self._cancel_login_timer()
            self._send_deferred_packets()
            self._set_keepalive_timer()

            if self._owner: # pragma: no branch
                self._owner.auth_completed()

            if self._acceptor:
                result = self._acceptor(self)

                if inspect.isawaitable(result):
                    self.create_task(result)

                self._acceptor = None
                self._error_handler = None

            if self._wait == 'auth' and self._waiter and \
                    not self._waiter.cancelled():
                self._waiter.set_result(None)
                self._wait = None
        else:
            raise ProtocolError('Unexpected userauth success response')

    def _process_userauth_banner(self, _pkttype: int, _pktid: int,
                                 packet: SSHPacket) -> None:
        """Process a user authentication banner message"""

        msg_bytes = packet.get_string()
        lang_bytes = packet.get_string()

        # Work around an extra NUL byte appearing in the user
        # auth banner message in some versions of cryptlib
        if b'cryptlib' in self._server_version and \
                packet.get_remaining_payload() == b'\0': # pragma: no cover
            packet.get_byte()

        packet.check_end()

        try:
            msg = msg_bytes.decode('utf-8')
            lang = lang_bytes.decode('ascii')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid userauth banner') from None

        self.logger.debug1('Received authentication banner')

        if self.is_client():
            cast(SSHClient, self._owner).auth_banner_received(msg, lang)
        else:
            raise ProtocolError('Unexpected userauth banner')

    def _process_global_request(self, _pkttype: int, _pktid: int,
                                packet: SSHPacket) -> None:
        """Process a global request"""

        request_bytes = packet.get_string()
        want_reply = packet.get_boolean()

        try:
            request = request_bytes.decode('ascii')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid global request') from None

        name = '_process_' + map_handler_name(request) + '_global_request'
        handler: PacketHandler | None = getattr(self, name, None)

        self._global_request_queue.append((handler, packet, want_reply))
        if len(self._global_request_queue) == 1:
            self._service_next_global_request()

    def _process_global_response(self, pkttype: int, _pktid: int,
                                 packet: SSHPacket) -> None:
        """Process a global response"""

        if self._global_request_waiters:
            waiter = self._global_request_waiters.pop(0)
            if not waiter.cancelled(): # pragma: no branch
                waiter.set_result((pkttype, packet))
        else:
            raise ProtocolError('Unexpected global response')

    def _process_channel_open(self, _pkttype: int, _pktid: int,
                              packet: SSHPacket) -> None:
        """Process a channel open request"""

        chantype_bytes = packet.get_string()
        send_chan = packet.get_uint32()
        send_window = packet.get_uint32()
        send_pktsize = packet.get_uint32()

        # Work around an off-by-one error in dropbear introduced in
        # https://github.com/mkj/dropbear/commit/49263b5
        if b'dropbear' in self._client_version and self._compressor:
            send_pktsize -= 1

        try:
            chantype = chantype_bytes.decode('ascii')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid channel open request') from None

        try:
            name = '_process_' + map_handler_name(chantype) + '_open'
            handler: _OpenHandler = getattr(self, name, None)

            if callable(handler):
                chan, session = handler(packet)
                chan.process_open(send_chan, send_window,
                                  send_pktsize, session)
            else:
                raise ChannelOpenError(OPEN_UNKNOWN_CHANNEL_TYPE,
                                       'Unknown channel type')
        except ChannelOpenError as exc:
            pass

            self.send_channel_open_failure(send_chan, exc.code,
                                           exc.reason, exc.lang)

    def _process_channel_open_confirmation(self, _pkttype: int, _pktid: int,
                                           packet: SSHPacket) -> None:
        """Process a channel open confirmation response"""

        recv_chan = packet.get_uint32()
        send_chan = packet.get_uint32()
        send_window = packet.get_uint32()
        send_pktsize = packet.get_uint32()

        # Work around an off-by-one error in dropbear introduced in
        # https://github.com/mkj/dropbear/commit/49263b5
        if b'dropbear' in self._server_version and self._compressor:
            send_pktsize -= 1

        chan = self._channels.get(recv_chan)
        if chan:
            chan.process_open_confirmation(send_chan, send_window,
                                           send_pktsize, packet)
        else:
            self.logger.debug1('Received open confirmation for unknown '
                               'channel %d', recv_chan)

            raise ProtocolError('Invalid channel number')

    def _process_channel_open_failure(self, _pkttype: int, _pktid: int,
                                      packet: SSHPacket) -> None:
        """Process a channel open failure response"""

        recv_chan = packet.get_uint32()
        code = packet.get_uint32()
        reason_bytes = packet.get_string()
        lang_bytes = packet.get_string()
        packet.check_end()

        try:
            reason = reason_bytes.decode('utf-8')
            lang = lang_bytes.decode('ascii')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid channel open failure') from None

        chan = self._channels.get(recv_chan)
        if chan:
            chan.process_open_failure(code, reason, lang)
        else:
            self.logger.debug1('Received open failure for unknown '
                               'channel %d', recv_chan)

            raise ProtocolError('Invalid channel number')

    def _process_keepalive_at_openssh_dot_com_global_request(
            self, packet: SSHPacket) -> None:
        """Process an incoming OpenSSH keepalive request"""

        packet.check_end()

        self.logger.debug2('Received OpenSSH keepalive request')
        self._report_global_response(True)

    _packet_handlers = {
        MSG_DISCONNECT:                 _process_disconnect,
        MSG_IGNORE:                     _process_ignore,
        MSG_UNIMPLEMENTED:              _process_unimplemented,
        MSG_DEBUG:                      _process_debug,
        MSG_SERVICE_REQUEST:            _process_service_request,
        MSG_SERVICE_ACCEPT:             _process_service_accept,
        MSG_EXT_INFO:                   _process_ext_info,

        MSG_KEXINIT:                    _process_kexinit,
        MSG_NEWKEYS:                    _process_newkeys,

        MSG_USERAUTH_REQUEST:           _process_userauth_request,
        MSG_USERAUTH_FAILURE:           _process_userauth_failure,
        MSG_USERAUTH_SUCCESS:           _process_userauth_success,
        MSG_USERAUTH_BANNER:            _process_userauth_banner,

        MSG_GLOBAL_REQUEST:             _process_global_request,
        MSG_REQUEST_SUCCESS:            _process_global_response,
        MSG_REQUEST_FAILURE:            _process_global_response,

        MSG_CHANNEL_OPEN:               _process_channel_open,
        MSG_CHANNEL_OPEN_CONFIRMATION:  _process_channel_open_confirmation,
        MSG_CHANNEL_OPEN_FAILURE:       _process_channel_open_failure
    }

    def abort(self) -> None:
        """Forcibly close the SSH connection

           This method closes the SSH connection immediately, without
           waiting for pending operations to complete and without sending
           an explicit SSH disconnect message. Buffered data waiting to be
           sent will be lost and no more data will be received. When the
           the connection is closed, :meth:`connection_lost()
           <SSHClient.connection_lost>` on the associated :class:`SSHClient`
           object will be called with the value `None`.

        """

        self.logger.info('Aborting connection')

        self._force_close(None)

    def close(self) -> None:
        """Cleanly close the SSH connection

           This method calls :meth:`disconnect` with the reason set to
           indicate that the connection was closed explicitly by the
           application.

        """

        self.logger.info('Closing connection')

        self.disconnect(DISC_BY_APPLICATION, 'Disconnected by application')

    async def wait_closed(self) -> None:
        """Wait for this connection to close

           This method is a coroutine which can be called to block until
           this connection has finished closing.

        """

        if self._agent:
            await self._agent.wait_closed()

        await self._close_event.wait()

    def disconnect(self, code: int, reason: str,
                   lang: str = DEFAULT_LANG) -> None:
        """Disconnect the SSH connection

           This method sends a disconnect message and closes the SSH
           connection after buffered data waiting to be written has been
           sent. No more data will be received. When the connection is
           fully closed, :meth:`connection_lost() <SSHClient.connection_lost>`
           on the associated :class:`SSHClient` or :class:`SSHServer` object
           will be called with the value `None`.

           :param code:
               The reason for the disconnect, from
               :ref:`disconnect reason codes <DisconnectReasons>`
           :param reason:
               A human readable reason for the disconnect
           :param lang:
               The language the reason is in
           :type code: `int`
           :type reason: `str`
           :type lang: `str`

        """

        for chan in list(self._channels.values()):
            chan.close()

        self._send_disconnect(code, reason, lang)
        self._force_close(None)

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Get additional information about the connection

           This method returns extra information about the connection once
           it is established. Supported values include everything supported
           by a socket transport plus:

             | host
             | port
             | username
             | client_version
             | server_version
             | send_cipher
             | send_mac
             | send_compression
             | recv_cipher
             | recv_mac
             | recv_compression

           See :meth:`get_extra_info() <asyncio.BaseTransport.get_extra_info>`
           in :class:`asyncio.BaseTransport` for more information.

           Additional information stored on the connection by calling
           :meth:`set_extra_info` can also be returned here.

        """

        return self._extra.get(name,
                               self._transport.get_extra_info(name, default)
                               if self._transport else default)

    def set_extra_info(self, **kwargs: Any) -> None:
        """Store additional information associated with the connection

           This method allows extra information to be associated with the
           connection. The information to store should be passed in as
           keyword parameters and can later be returned by calling
           :meth:`get_extra_info` with one of the keywords as the name
           to retrieve.

        """

        self._extra.update(**kwargs)

    def set_keepalive(self, interval: float | str | None = None,
                      count_max: int | None = None) -> None:
        """set keep-alive timer on this connection

           This method sets the parameters of the keepalive timer on the
           connection. If *interval* is set to a non-zero value,
           keep-alive requests will be sent whenever the connection is
           idle, and if a response is not received after *count_max*
           attempts, the connection is closed.

           :param interval: (optional)
               The time in seconds to wait before sending a keep-alive message
               if no data has been received. This defaults to 0, which
               disables sending these messages.
           :param count_max: (optional)
               The maximum number of keepalive messages which will be sent
               without getting a response before closing the connection.
               This defaults to 3, but only applies when *interval* is
               non-zero.
           :type interval: `int`, `float`, or `str`
           :type count_max: `int`

        """

        if interval is not None:
            if isinstance(interval, str):
                interval = parse_time_interval(interval)

            if interval < 0:
                raise ValueError('Keepalive interval cannot be negative')

            self._keepalive_interval = interval

        if count_max is not None:
            if count_max < 0:
                raise ValueError('Keepalive count max cannot be negative')

            self._keepalive_count_max = count_max

        self._reset_keepalive_timer()

    def send_debug(self, msg: str, lang: str = DEFAULT_LANG,
                   always_display: bool = False) -> None:
        """Send a debug message on this connection

           This method can be called to send a debug message to the
           other end of the connection.

           :param msg:
               The debug message to send
           :param lang:
               The language the message is in
           :param always_display:
               Whether or not to display the message
           :type msg: `str`
           :type lang: `str`
           :type always_display: `bool`

        """

        self.logger.debug1('Sending debug message: %s%s', msg,
                           ' (always display)' if always_display else '')

        self.send_packet(MSG_DEBUG, Boolean(always_display),
                         String(msg), String(lang))

    def create_tcp_channel(self, encoding: str | None = None,
                           errors: str = 'strict',
                           window: int = DEFAULT_WINDOW,
                           max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> \
            SSHTCPChannel:
        """Create an SSH TCP channel for a new direct TCP connection

           This method can be called by :meth:`connection_requested()
           <SSHServer.connection_requested>` to create an
           :class:`SSHTCPChannel` with the desired encoding, Unicode
           error handling strategy, window, and max packet size for
           a newly created SSH direct connection.

           :param encoding: (optional)
               The Unicode encoding to use for data exchanged on the
               connection. This defaults to `None`, allowing the
               application to send and receive raw bytes.
           :param errors: (optional)
               The error handling strategy to apply on encode/decode errors
           :param window: (optional)
               The receive window size for this session
           :param max_pktsize: (optional)
               The maximum packet size for this session
           :type encoding: `str` or `None`
           :type errors: `str`
           :type window: `int`
           :type max_pktsize: `int`

           :returns: :class:`SSHTCPChannel`

        """

        return SSHTCPChannel(self, self._loop, encoding,
                             errors, window, max_pktsize)

    def create_unix_channel(self, encoding: str | None = None,
                            errors: str = 'strict',
                            window: int = DEFAULT_WINDOW,
                            max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> \
            SSHUNIXChannel:
        """Create an SSH UNIX channel for a new direct UNIX domain connection

           This method can be called by :meth:`unix_connection_requested()
           <SSHServer.unix_connection_requested>` to create an
           :class:`SSHUNIXChannel` with the desired encoding, Unicode
           error handling strategy, window, and max packet size for
           a newly created SSH direct UNIX domain socket connection.

           :param encoding: (optional)
               The Unicode encoding to use for data exchanged on the
               connection. This defaults to `None`, allowing the
               application to send and receive raw bytes.
           :param errors: (optional)
               The error handling strategy to apply on encode/decode errors
           :param window: (optional)
               The receive window size for this session
           :param max_pktsize: (optional)
               The maximum packet size for this session
           :type encoding: `str` or `None`
           :type errors: `str`
           :type window: `int`
           :type max_pktsize: `int`

           :returns: :class:`SSHUNIXChannel`

        """

        return SSHUNIXChannel(self, self._loop, encoding,
                              errors, window, max_pktsize)

    def create_tuntap_channel(self, window: int = DEFAULT_WINDOW,
                              max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> \
            SSHTunTapChannel:
        """Create a channel to use for TUN/TAP forwarding

           This method can be called by :meth:`tun_requested()
           <SSHServer.tun_requested>` or :meth:`tap_requested()
           <SSHServer.tap_requested>` to create an :class:`SSHTunTapChannel`
           with the desired window and max packet size for a newly created
           TUN/TAP tunnel.

           :param window: (optional)
               The receive window size for this session
           :param max_pktsize: (optional)
               The maximum packet size for this session
           :type window: `int`
           :type max_pktsize: `int`

           :returns: :class:`SSHTunTapChannel`

        """

        return SSHTunTapChannel(self, self._loop, None, 'strict',
                                window, max_pktsize)

    def create_x11_channel(
            self, window: int = DEFAULT_WINDOW,
            max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> SSHX11Channel:
        """Create an SSH X11 channel to use in X11 forwarding"""

        return SSHX11Channel(self, self._loop, None, 'strict',
                             window, max_pktsize)

    def create_agent_channel(
            self, window: int = DEFAULT_WINDOW,
            max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> SSHAgentChannel:
        """Create an SSH agent channel to use in agent forwarding"""

        return SSHAgentChannel(self, self._loop, None, 'strict',
                               window, max_pktsize)

    async def create_connection(
            self, session_factory: SSHTCPSessionFactory[AnyStr],
            remote_host: str, remote_port: int, orig_host: str = '',
            orig_port: int = 0, *, encoding: str | None = None,
            errors: str = 'strict', window: int = DEFAULT_WINDOW,
            max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> \
                tuple[SSHTCPChannel[AnyStr], SSHTCPSession[AnyStr]]:
        """Create an SSH direct or forwarded TCP connection"""

        raise NotImplementedError

    async def create_unix_connection(
            self, session_factory: SSHUNIXSessionFactory[AnyStr],
            remote_path: str, *, encoding: str | _next_recv_encryption = None,
            errors: str = 'strict', window: int = DEFAULT_WINDOW,
            max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> \
                tuple[SSHUNIXChannel[AnyStr], SSHUNIXSession[AnyStr]]:

        """Create an SSH direct or forwarded UNIX domain socket connection"""

        raise NotImplementedError

    async def forward_connection(
            self, dest_host: str, dest_port: int) -> SSHForwarder:
        """Forward a tunneled TCP connection

           This method is a coroutine which can be returned by a
           `session_factory` to forward connections tunneled over
           SSH to the specified destination host and port.

           :param dest_host:
               The hostname or address to forward the connections to
           :param dest_port:
               The port number to forward the connections to
           :type dest_host: `str` or `None`
           :type dest_port: `int`

           :returns: :class:`asyncio.BaseProtocol`

        """

        try:
            _, peer = await self._loop.create_connection(SSHForwarder,
                                                         dest_host, dest_port)
        except OSError as exc:
            raise ChannelOpenError(OPEN_CONNECT_FAILED, str(exc)) from None

        return SSHForwarder(peer)

    async def forward_unix_connection(self, dest_path: str) -> SSHForwarder:
        """Forward a tunneled UNIX domain socket connection

           This method is a coroutine which can be returned by a
           `session_factory` to forward connections tunneled over
           SSH to the specified destination path.

           :param dest_path:
               The path to forward the connection to
           :type dest_path: `str`

           :returns: :class:`asyncio.BaseProtocol`

        """

        try:
            _, peer = \
                await self._loop.create_unix_connection(SSHForwarder, dest_path)

        except OSError as exc:
            raise ChannelOpenError(OPEN_CONNECT_FAILED, str(exc)) from None

        return SSHForwarder(peer)

    @async_context_manager
    async def forward_local_port(
            self, listen_host: str, listen_port: int,
            dest_host: str, dest_port: int,
            accept_handler: SSHAcceptHandler | None = None) -> SSHListener:
        """set up local port forwarding

           This method is a coroutine which attempts to set up port
           forwarding from a local listening port to a remote host and port
           via the SSH connection. If the request is successful, the
           return value is an :class:`SSHListener` object which can be used
           later to shut down the port forwarding.

           :param listen_host:
               The hostname or address on the local host to listen on
           :param listen_port:
               The port number on the local host to listen on
           :param dest_host:
               The hostname or address to forward the connections to
           :param dest_port:
               The port number to forward the connections to
           :param accept_handler:
               A `callable` or coroutine which takes arguments of the
               original host and port of the client and decides whether
               or not to allow connection forwarding, returning `True` to
               accept the connection and begin forwarding or `False` to
               reject and close it.
           :type listen_host: `str`
           :type listen_port: `int`
           :type dest_host: `str`
           :type dest_port: `int`
           :type accept_handler: `callable` or coroutine

           :returns: :class:`SSHListener`

           :raises: :exc:`OSError` if the listener can't be opened

        """

        async def tunnel_connection(
                session_factory: SSHTCPSessionFactory[bytes],
                orig_host: str, orig_port: int) -> \
                    tuple[SSHTCPChannel[bytes], SSHTCPSession[bytes]]:
            """Forward a local connection over SSH"""

            if accept_handler:
                result = accept_handler(orig_host, orig_port)

                if inspect.isawaitable(result):
                    result = await cast(Awaitable[bool], result)

                if not result:
                    self.logger.info('Request for TCP forwarding from '
                                     '%s to %s denied by application',
                                     (orig_host, orig_port),
                                     (dest_host, dest_port))

                    raise ChannelOpenError(OPEN_ADMINISTRATIVELY_PROHIBITED,
                                           'Connection forwarding denied')

            return await self.create_connection(session_factory, dest_host,
                                                dest_port, orig_host, orig_port)

        try:
            listener = await create_tcp_forward_listener(self, self._loop,
                                                         tunnel_connection,
                                                         listen_host,
                                                         listen_port)
        except OSError as exc:
            self.logger.debug1('Failed to create local TCP listener: %s', exc)
            raise

        if listen_port == 0:
            listen_port = listener.get_port()

        if dest_port == 0:
            dest_port = listen_port

        self._local_listeners[listen_host, listen_port] = listener

        return listener

    @async_context_manager
    async def forward_local_path(self, listen_path: str,
                                 dest_path: str) -> SSHListener:
        """set up local UNIX domain socket forwarding

           This method is a coroutine which attempts to set up UNIX domain
           socket forwarding from a local listening path to a remote path
           via the SSH connection. If the request is successful, the
           return value is an :class:`SSHListener` object which can be used
           later to shut down the UNIX domain socket forwarding.

           :param listen_path:
               The path on the local host to listen on
           :param dest_path:
               The path on the remote host to forward the connections to
           :type listen_path: `str`
           :type dest_path: `str`

           :returns: :class:`SSHListener`

           :raises: :exc:`OSError` if the listener can't be opened

        """

        async def tunnel_connection(
                session_factory: SSHUNIXSessionFactory[bytes]) -> \
                    tuple[SSHUNIXChannel[bytes], SSHUNIXSession[bytes]]:
            """Forward a local connection over SSH"""

            return await self.create_unix_connection(session_factory,
                                                     dest_path)

        try:
            listener = await create_unix_forward_listener(self, self._loop,
                                                          tunnel_connection,
                                                          listen_path)
        except OSError as exc:
            raise

        self._local_listeners[listen_path] = listener

        return listener

    def forward_tuntap(self, mode: int, unit: int | None) -> SSHForwarder:
        """set up TUN/TAP forwarding"""

        try:
            transport, peer = create_tuntap(SSHForwarder, mode, unit)
            interface = transport.get_extra_info('interface')

        except OSError as exc:
            raise ChannelOpenError(OPEN_CONNECT_FAILED, str(exc)) from None

        return SSHForwarder(peer,
                            extra={'interface': interface})

    def close_forward_listener(self, listen_key: ListenKey) -> None:
        """Mark a local forwarding listener as closed"""

        self._local_listeners.pop(listen_key, None)

    def detach_x11_listener(self, chan: SSHChannel[AnyStr]) -> None:
        """Detach a session from a local X11 listener"""

        raise NotImplementedError