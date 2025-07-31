import asyncio
import inspect
import io
import os
import shlex
import socket
from pathlib import PurePath
from subprocess import PIPE
from pathlib import Path
from typing import (
    Optional, 
    Sequence, 
    TYPE_CHECKING, 
    Awaitable, 
    AnyStr,
)
from .async_file_protocol import AsyncFileProtocol
from .async_context_manager import async_context_manager
from .constants import (
    MIN_SFTP_VERSION,
    SSH_TUN_MODE_ETHERNET,
    SSH_TUN_MODE_POINTTOPOINT,
    MSG_REQUEST_SUCCESS,
    OPEN_CONNECT_FAILED,
    OPEN_ADMINISTRATIVELY_PROHIBITED,
    DEFAULT_WINDOW,
    DEFAULT_MAX_PKTSIZE,
    DEFAULT_PORT,
)
from .error import (
    ChannelOpenError,
    ProtocolError,
)
from .ssh_connection import SSHConnection
from .packet import String, UInt32, SSHPacket
from .types import DefTuple, ListenKey, MaybeAwait
from .ssh_reader import SSHReader
from .ssh_writer import SSHWriter
from .types import IO


File = IO[bytes] | AsyncFileProtocol[bytes]
ProcessSource = int | str | socket.socket | PurePath | SSHReader[bytes] | asyncio.StreamReader | File
ProcessTarget = int | str | socket.socket | PurePath | SSHWriter[bytes] | asyncio.StreamWriter | File


if TYPE_CHECKING:
    from .ssh_channel import SSHChannel
    from .ssh_client_session import SSHClientSession
    from .ssh_forwarder import SSHForwarder
    from .sftp_client import SFTPClient
    from .ssh_listener import SSHListener
    from .ssh_public_key import SSHKey, SSHKeyPair, KeyPairListArg
    from .ssh_tcp_channel import SSHTCPChannel, SSHTCPSession, SSHTCPSessionFactory
    from .ssh_unix_channel import SSHUNIXChannel, SSHUNIXSession, SSHUNIXSessionFactory
    from .ssh_tun_tap_channel import SSHTunTapChannel, SSHTunTapSession, SSHTunTapSessionFactory


class SSHClientConnection(SSHConnection):
    """SSH client connection

       This class represents an SSH client connection.

       Once authentication is successful on a connection, new client
       sessions can be opened by calling :meth:`create_session`.

       Direct TCP connections can be opened by calling
       :meth:`create_connection`.

       Remote listeners for forwarded TCP connections can be opened by
       calling :meth:`create_server`.

       Direct UNIX domain socket connections can be opened by calling
       :meth:`create_unix_connection`.

       Remote listeners for forwarded UNIX domain socket connections
       can be opened by calling :meth:`create_unix_server`.

       TCP port forwarding can be set up by calling :meth:`forward_local_port`
       or :meth:`forward_remote_port`.

       UNIX domain socket forwarding can be set up by calling
       :meth:`forward_local_path` or :meth:`forward_remote_path`.

       Mixed forwarding from a TCP port to a UNIX domain socket or
       vice-versa can be set up by calling :meth:`forward_local_port_to_path`,
       :meth:`forward_local_path_to_port`,
       :meth:`forward_remote_port_to_path`, or
       :meth:`forward_remote_path_to_port`.

    """

    _options: 'SSHClientConnectionOptions'
    _owner: SSHClient
    _x11_listener: Optional[SSHX11ClientListener]

    def __init__(self, loop: asyncio.AbstractEventLoop,
                 options: 'SSHClientConnectionOptions',
                 acceptor: _AcceptHandler = None,
                 error_handler: _ErrorHandler = None,
                 wait: Optional[str] = None):
        super().__init__(loop, options, acceptor, error_handler,
                         wait, server=False)

        self._host = options.host
        self._port = options.port

        self._known_hosts = options.known_hosts
        self._host_key_alias = options.host_key_alias

        self._server_host_key_algs: Optional[Sequence[bytes]] = None
        self._server_host_key: Optional[SSHKey] = None

        self._server_host_keys_handler = options.server_host_keys_handler

        self._username = options.username
        self._password = options.password

        self._client_host_keys: list[_ClientHostKey] = []

        self._client_keys: list[SSHKeyPair] = \
            list(options.client_keys) if options.client_keys else []
        self._saved_rsa_key: Optional[_ClientHostKey] = None

        if options.preferred_auth != ():
            self._preferred_auth = [method.encode('ascii') for method in
                                    options.preferred_auth]
        else:
            self._preferred_auth = get_supported_client_auth_methods()

        self._disable_trivial_auth = options.disable_trivial_auth

        if options.agent_path is not None:
            self._agent = SSHAgentClient(options.agent_path)

        self._agent_identities = options.agent_identities
        self._agent_forward_path = options.agent_forward_path
        self._get_agent_keys = bool(self._agent)

        self._pkcs11_provider = options.pkcs11_provider
        self._pkcs11_pin = options.pkcs11_pin
        self._get_pkcs11_keys = bool(self._pkcs11_provider)

        gss_host = options.gss_host if options.gss_host != () else options.host

        if gss_host:
            try:
                self._gss = GSSClient(gss_host, options.gss_store,
                                      options.gss_delegate_creds)
                self._gss_kex = options.gss_kex
                self._gss_auth = options.gss_auth
                self._gss_mic_auth = self._gss_auth
            except GSSError:
                pass

        self._kbdint_password_auth = False

        self._remote_listeners: \
            dict[ListenKey, Union[SSHTCPClientListener,
                                  SSHUNIXClientListener]] = {}

        self._dynamic_remote_listeners: dict[str, SSHTCPClientListener] = {}

    def _connection_made(self) -> None:
        """Handle the opening of a new connection"""

        assert self._transport is not None

        if not self._host:
            if self._peer_addr:
                self._host = self._peer_addr
                self._port = self._peer_port
            else:
                remote_peer = self.get_extra_info('remote_peername')
                self._host, self._port = remote_peer

        if self._options.client_host_keysign:
            sock: socket.socket = self._transport.get_extra_info('socket')

            self._client_host_keys = list(get_keysign_keys(
                self._options.client_host_keysign, sock.fileno(),
                self._options.client_host_pubkeys))
        elif self._options.client_host_keypairs:
            self._client_host_keys = list(self._options.client_host_keypairs)
        else:
            self._client_host_keys = []

        if self._known_hosts is None:
            self._trusted_host_keys = None
            self._trusted_ca_keys = None
        else:
            if not self._known_hosts:
                default_known_hosts = Path('~', '.ssh',
                                           'known_hosts').expanduser()

                if (default_known_hosts.is_file() and
                        os.access(default_known_hosts, os.R_OK)):
                    self._known_hosts = str(default_known_hosts)
                else:
                    self._known_hosts = b''

            port = self._port if self._port != DEFAULT_PORT else None

            self._match_known_hosts(self._known_hosts,
                                    self._host_key_alias or self._host,
                                    self._peer_addr, port)

        default_host_key_algs = []

        if self._options.server_host_key_algs != 'default':
            if self._trusted_host_key_algs:
                default_host_key_algs = self._trusted_host_key_algs

            if self._trusted_ca_keys:
                default_host_key_algs = \
                    get_default_certificate_algs() + default_host_key_algs

        if not default_host_key_algs:
            default_host_key_algs = \
                get_default_certificate_algs() + get_default_public_key_algs()

        if self._x509_trusted_certs is not None:
            if self._x509_trusted_certs or self._x509_trusted_cert_paths:
                default_host_key_algs = \
                    get_default_x509_certificate_algs() + default_host_key_algs

        self._server_host_key_algs = _select_host_key_algs(
            self._options.server_host_key_algs,
            self._options.config.get('HostKeyAlgorithms', ()),
            default_host_key_algs,
        )

        self.logger.info('Connected to SSH server at %s',
                         (self._host, self._port))

        if self._options.proxy_command:
            proxy_command = ' '.join(shlex.quote(arg) for arg in
                                     self._options.proxy_command)
            self.logger.info('  Proxy command: %s', proxy_command)
        else:
            self.logger.info('  Local address: %s',
                             (self._local_addr, self._local_port))
            self.logger.info('  Peer address: %s',
                             (self._peer_addr, self._peer_port))


    def _cleanup(self, exc: Optional[Exception]) -> None:
        """Clean up this client connection"""

        if self._agent:
            self._agent.close()

        if self._remote_listeners:
            for tcp_listener in list(self._remote_listeners.values()):
                tcp_listener.close()

            self._remote_listeners = {}
            self._dynamic_remote_listeners = {}

        if exc is None:
            self.logger.info('Connection closed')
        elif isinstance(exc, ConnectionLost):
            self.logger.info(str(exc))
        else:
            self.logger.info('Connection failure: ' + str(exc))

        super()._cleanup(exc)


    def _choose_signature_alg(self, keypair: _ClientHostKey) -> bool:
        """Choose signature algorithm to use for key-based authentication"""

        if self._server_sig_algs:
            for alg in keypair.sig_algorithms:
                if keypair.use_webauthn and not alg.startswith(b'webauthn-'):
                    continue

                if alg in self._sig_algs and alg in self._server_sig_algs:
                    keypair.set_sig_algorithm(alg)
                    return True

        return keypair.sig_algorithms[-1] in self._sig_algs

    def validate_server_host_key(self, key_data: bytes) -> SSHKey:
        """Validate and return the server's host key"""

        try:
            host_key = self._validate_host_key(
                self._host_key_alias or self._host,
                self._peer_addr, self._port, key_data)
        except ValueError as exc:
            host = self._host

            if self._host_key_alias:
                host += f' with alias {self._host_key_alias}'

            raise HostKeyNotVerifiable(f'{exc} for host {host}') from None

        self._server_host_key = host_key
        return host_key

    def get_server_host_key(self) -> Optional[SSHKey]:
        """Return the server host key used in the key exchange

           This method returns the server host key used to complete the
           key exchange with the server.

           If GSS key exchange is used, `None` is returned.

           :returns: An :class:`SSHKey` public key or `None`

        """

        return self._server_host_key

    def get_server_auth_methods(self) -> Sequence[str]:
        """Return the server host key used in the key exchange

           This method returns the auth methods available to authenticate
           to the server.

           :returns: `list` of `str`

        """

        return [method.decode('ascii') for method in self._auth_methods]

    def try_next_auth(self, *, next_method: bool = False) -> None:
        """Attempt client authentication using the next compatible method"""

        if self._auth:
            self._auth.cancel()
            self._auth = None

        if next_method:
            self._auth_methods.pop(0)

        while self._auth_methods:
            self._auth = lookup_client_auth(self, self._auth_methods[0])

            if self._auth:
                return

            self._auth_methods.pop(0)

        self.logger.info('Auth failed for user %s', self._username)

        self._force_close(PermissionDenied('Permission denied for user '
                                           f'{self._username} on host '
                                           f'{self._host}'))

    def gss_kex_auth_requested(self) -> bool:
        """Return whether to allow GSS key exchange authentication or not"""

        if self._gss_kex_auth:
            self._gss_kex_auth = False
            return True
        else:
            return False

    def gss_mic_auth_requested(self) -> bool:
        """Return whether to allow GSS MIC authentication or not"""

        if self._gss_mic_auth:
            self._gss_mic_auth = False
            return True
        else:
            return False

    async def host_based_auth_requested(self) -> \
            tuple[Optional[_ClientHostKey], str, str]:
        """Return a host key, host, and user to authenticate with"""

        if not self._host_based_auth:
            return None, '', ''

        key: Optional[_ClientHostKey]

        while True:
            if self._saved_rsa_key:
                key = self._saved_rsa_key
                key.algorithm = key.sig_algorithm + b'-cert-v01@openssh.com'
                self._saved_rsa_key = None
            else:
                try:
                    key = self._client_host_keys.pop(0)
                except IndexError:
                    key = None
                    break

            assert key is not None

            if self._choose_signature_alg(key):
                if key.algorithm == b'ssh-rsa-cert-v01@openssh.com' and \
                        key.sig_algorithm != b'ssh-rsa':
                    self._saved_rsa_key = key

                break

        client_host = self._options.client_host

        if client_host is None:
            sockname: SockAddr = self.get_extra_info('sockname')

            if sockname:
                try:
                    client_host, _ = await self._loop.getnameinfo(
                        sockname, socket.NI_NUMERICSERV)
                except socket.gaierror:
                    client_host = sockname[0]
            else:
                client_host = ''

        # Add a trailing '.' to the client host to be compatible with
        # ssh-keysign from OpenSSH
        if self._options.client_host_keysign and client_host[-1:] != '.':
            client_host += '.'

        return key, client_host, self._options.client_username

    async def public_key_auth_requested(self) -> Optional[SSHKeyPair]:
        """Return a client key pair to authenticate with"""

        if not self._public_key_auth:
            return None

        if self._get_agent_keys:
            assert self._agent is not None

            try:
                agent_keys = await self._agent.get_keys(self._agent_identities)
                self._client_keys[:0] = list(agent_keys)
            except ValueError:
                pass

            self._get_agent_keys = False

        if self._get_pkcs11_keys:
            assert self._pkcs11_provider is not None

            pkcs11_keys = await self._loop.run_in_executor(
                None, load_pkcs11_keys, self._pkcs11_provider, self._pkcs11_pin)

            self._client_keys[:0] = list(pkcs11_keys)
            self._get_pkcs11_keys = False

        while True:
            if not self._client_keys:
                result = self._owner.public_key_auth_requested()

                if inspect.isawaitable(result):
                    result: KeyPairListArg = await result

                if not result:
                    return None

                self._client_keys = list(load_keypairs(result))

            # OpenSSH versions before 7.8 didn't support RSA SHA-2
            # signature names in certificate key types, requiring the
            # use of ssh-rsa-cert-v01@openssh.com as the key type even
            # when using SHA-2 signatures. However, OpenSSL 8.8 and
            # later reject ssh-rsa-cert-v01@openssh.com as a key type
            # by default, requiring that the RSA SHA-2 version of the key
            # type be used. This makes it difficult to use RSA keys with
            # certificates without knowing the version of the remote
            # server and which key types it will accept.
            #
            # The code below works around this by trying multiple key
            # types during public key and host-based authentication when
            # using SHA-2 signatures with RSA keys signed by certificates.

            if self._saved_rsa_key:
                key = self._saved_rsa_key
                key.algorithm = key.sig_algorithm + b'-cert-v01@openssh.com'
                self._saved_rsa_key = None
            else:
                key = self._client_keys.pop(0)

            if self._choose_signature_alg(key):
                if key.algorithm == b'ssh-rsa-cert-v01@openssh.com' and \
                        key.sig_algorithm != b'ssh-rsa':
                    self._saved_rsa_key = key

                return key

    async def password_auth_requested(self) -> Optional[str]:
        """Return a password to authenticate with"""

        if not self._password_auth and not self._kbdint_password_auth:
            return None

        if self._password is not None:
            password: Optional[str] = self._password

            if callable(password):
                password: str | None = password()

            if inspect.isawaitable(password):
                password: str | None = await password
            else:
                password = password

            self._password = None
        else:
            result = self._owner.password_auth_requested()

            if inspect.isawaitable(result):
                password: str | None = await result
            else:
                password: str | None = result

        return password

    async def password_change_requested(self, prompt: str,
                                        lang: str) -> tuple[str, str]:
        """Return a password to authenticate with and what to change it to"""

        result = self._owner.password_change_requested(prompt, lang)

        if inspect.isawaitable(result):
            result: PasswordChangeResponse = await result

        return result

    def password_changed(self) -> None:
        """Report a successful password change"""

        self._owner.password_changed()

    def password_change_failed(self) -> None:
        """Report a failed password change"""

        self._owner.password_change_failed()

    async def kbdint_auth_requested(self) -> Optional[str]:
        """Return the list of supported keyboard-interactive auth methods

           If keyboard-interactive auth is not supported in the client but
           a password was provided when the connection was opened, this
           will allow sending the password via keyboard-interactive auth.

        """

        if not self._kbdint_auth:
            return None

        result = self._owner.kbdint_auth_requested()

        if inspect.isawaitable(result):
            result: str | None = await result

        if result is NotImplemented:
            if self._password is not None and not self._kbdint_password_auth:
                self._kbdint_password_auth = True
                result = ''
            else:
                result = None

        return result

    async def kbdint_challenge_received(
            self, name: str, instructions: str, lang: str,
            prompts: KbdIntPrompts) -> Optional[KbdIntResponse]:
        """Return responses to a keyboard-interactive auth challenge"""

        if self._kbdint_password_auth:
            if not prompts:
                # Silently drop any empty challenges used to print messages
                response: Optional[KbdIntResponse] = []
            elif len(prompts) == 1:
                prompt = prompts[0][0].lower()

                if 'password' in prompt or 'passcode' in prompt:
                    password = await self.password_auth_requested()

                    response = [password] if password is not None else None
                else:
                    response = None
            else:
                response = None
        else:
            result = self._owner.kbdint_challenge_received(name, instructions,
                                                           lang, prompts)

            if inspect.isawaitable(result):
                response = await result
            else:
                response = result

        return response

    def _process_session_open(self, _packet: SSHPacket) -> \
            tuple[SSHServerChannel, SSHServerSession]:
        """Process an inbound session open request

           These requests are disallowed on an SSH client.

        """

        # pylint: disable=no-self-use

        raise ChannelOpenError(OPEN_ADMINISTRATIVELY_PROHIBITED,
                               'Session open forbidden on client')

    def _process_direct_tcpip_open(self, _packet: SSHPacket) -> \
            tuple[SSHTCPChannel[bytes], SSHTCPSession[bytes]]:
        """Process an inbound direct TCP/IP channel open request

           These requests are disallowed on an SSH client.

        """

        # pylint: disable=no-self-use

        raise ChannelOpenError(OPEN_ADMINISTRATIVELY_PROHIBITED,
                               'Direct TCP/IP open forbidden on client')

    def _process_forwarded_tcpip_open(self, packet: SSHPacket) -> \
            tuple[SSHTCPChannel, MaybeAwait[SSHTCPSession]]:
        """Process an inbound forwarded TCP/IP channel open request"""

        dest_host_bytes = packet.get_string()
        dest_port = packet.get_uint32()
        orig_host_bytes = packet.get_string()
        orig_port = packet.get_uint32()
        packet.check_end()

        try:
            dest_host = dest_host_bytes.decode('utf-8')
            orig_host = orig_host_bytes.decode('utf-8')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid forwarded TCP/IP channel '
                                'open request') from None

        # Some buggy servers send back a port of `0` instead of the actual
        # listening port when reporting connections which arrive on a listener
        # set up on a dynamic port. This lookup attempts to work around that.
        listener = (
            self._remote_listeners.get((dest_host, dest_port)) or
            self._dynamic_remote_listeners.get(dest_host)
        )

        if listener:
            chan, session = listener.process_connection(orig_host, orig_port)

            return chan, session
        else:
            raise ChannelOpenError(OPEN_CONNECT_FAILED, 'No such listener')

    async def close_client_tcp_listener(self, listen_host: str,
                                        listen_port: int) -> None:
        """Close a remote TCP/IP listener"""

        await self._make_global_request(
            b'cancel-tcpip-forward', String(listen_host), UInt32(listen_port))

        self.logger.info('Closed remote TCP listener on %s',
                         (listen_host, listen_port))

        listener = self._remote_listeners.get((listen_host, listen_port))

        if listener:
            if self._dynamic_remote_listeners.get(listen_host) == listener:
                del self._dynamic_remote_listeners[listen_host]

            del self._remote_listeners[listen_host, listen_port]

    def _process_direct_streamlocal_at_openssh_dot_com_open(
            self, _packet: SSHPacket) -> \
                tuple[SSHUNIXChannel, SSHUNIXSession]:
        """Process an inbound direct UNIX domain channel open request

           These requests are disallowed on an SSH client.

        """

        # pylint: disable=no-self-use

        raise ChannelOpenError(OPEN_ADMINISTRATIVELY_PROHIBITED,
                               'Direct UNIX domain socket open '
                               'forbidden on client')

    def _process_tun_at_openssh_dot_com_open(
            self, _packet: SSHPacket) -> \
                tuple[SSHTunTapChannel, SSHTunTapSession]:
        """Process an inbound TUN/TAP open request

           These requests are disallowed on an SSH client.

        """

        # pylint: disable=no-self-use

        raise ChannelOpenError(OPEN_ADMINISTRATIVELY_PROHIBITED,
                               'TUN/TAP request forbidden on client')

    def _process_forwarded_streamlocal_at_openssh_dot_com_open(
            self, packet: SSHPacket) -> \
                tuple[SSHUNIXChannel, MaybeAwait[SSHUNIXSession]]:
        """Process an inbound forwarded UNIX domain channel open request"""

        dest_path_bytes = packet.get_string()
        _ = packet.get_string()                         # reserved
        packet.check_end()

        try:
            dest_path = dest_path_bytes.decode('utf-8')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid forwarded UNIX domain channel '
                                'open request') from None

        listener: SSHUNIXClientListener[bytes] = self._remote_listeners.get(dest_path)

        if listener:
            chan, session = listener.process_connection()

            self.logger.info('Accepted remote UNIX connection on %s', dest_path)

            return chan, session
        else:
            raise ChannelOpenError(OPEN_CONNECT_FAILED, 'No such listener')

    async def close_client_unix_listener(self, listen_path: str) -> None:
        """Close a remote UNIX domain socket listener"""

        await self._make_global_request(
            b'cancel-streamlocal-forward@openssh.com', String(listen_path))

        self.logger.info('Closed UNIX listener on %s', listen_path)

        if listen_path in self._remote_listeners:
            del self._remote_listeners[listen_path]

    def _process_x11_open(self, packet: SSHPacket) -> \
            tuple[SSHX11Channel, Awaitable[SSHX11ClientForwarder]]:
        """Process an inbound X11 channel open request"""

        orig_host_bytes = packet.get_string()
        orig_port = packet.get_uint32()

        packet.check_end()

        try:
            orig_host = orig_host_bytes.decode('utf-8')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid forwarded X11 channel '
                                'open request') from None

        if self._x11_listener:
            chan = self.create_x11_channel()

            chan.set_inbound_peer_names(orig_host, orig_port)

            return chan, self._x11_listener.forward_connection()
        else:
            raise ChannelOpenError(OPEN_CONNECT_FAILED,
                                   'X11 forwarding disabled')

    def _process_auth_agent_at_openssh_dot_com_open(
            self, packet: SSHPacket) -> \
                tuple[SSHUNIXChannel, Awaitable[SSHForwarder]]:
        """Process an inbound auth agent channel open request"""

        packet.check_end()

        if self._agent_forward_path:
            return (self.create_unix_channel(),
                    self.forward_unix_connection(self._agent_forward_path))
        else:
            raise ChannelOpenError(OPEN_CONNECT_FAILED,
                                   'Auth agent forwarding disabled')

    def _process_hostkeys_00_at_openssh_dot_com_global_request(
            self, packet: SSHPacket) -> None:
        """Process a list of accepted server host keys"""

        self.create_task(self._finish_hostkeys(packet))

    async def _finish_hostkeys(self, packet: SSHPacket) -> None:
        """Finish processing hostkeys global request"""

        if not self._server_host_keys_handler:
            self._report_global_response(False)
            return

        if self._trusted_host_keys is None:
            self._report_global_response(False)
            return

        added = []
        removed = list(self._trusted_host_keys)
        retained = []
        revoked = []
        prove = []

        while packet:
            try:
                key_data = packet.get_string()
                key = decode_ssh_public_key(key_data)

                if key in self._revoked_host_keys:
                    revoked.append(key)
                elif key in self._trusted_host_keys:
                    retained.append(key)
                    removed.remove(key)
                else:
                    prove.append((key, String(key_data)))
            except KeyImportError:
                pass

        if prove:
            pkttype, packet = await self._make_global_request(
                b'hostkeys-prove-00@openssh.com',
                b''.join(key_str for _, key_str in prove))

            if pkttype == MSG_REQUEST_SUCCESS:
                prefix = String('hostkeys-prove-00@openssh.com') + \
                         String(self._session_id)

                for key, key_str in prove:
                    sig = packet.get_string()

                    if key.verify(prefix + key_str, sig):
                        added.append(key)

        packet.check_end()


        result = self._server_host_keys_handler(added, removed,
                                                retained, revoked)

        if inspect.isawaitable(result):
            await result

        self._report_global_response(True)

    async def attach_x11_listener(self, chan: SSHClientChannel[AnyStr],
                                  display: Optional[str],
                                  auth_path: Optional[str],
                                  single_connection: bool) -> \
            tuple[bytes, bytes, int]:
        """Attach a channel to a local X11 display"""

        if not display:
            display = os.environ.get('DISPLAY')

        if not display:
            raise ValueError('X11 display not set')

        if not self._x11_listener:
            self._x11_listener = await create_x11_client_listener(
                self._loop, display, auth_path)

        return self._x11_listener.attach(display, chan, single_connection)

    def detach_x11_listener(self, chan: SSHChannel[AnyStr]) -> None:
        """Detach a session from a local X11 listener"""

        if self._x11_listener:
            if self._x11_listener.detach(chan):
                self._x11_listener = None

    async def create_session(self, session_factory: SSHClientSessionFactory,
                             command: DefTuple[Optional[str]] = (), *,
                             subsystem: DefTuple[Optional[str]]= (),
                             env: DefTuple[Optional[Env]] = (),
                             send_env: DefTuple[Optional[EnvSeq]] = (),
                             request_pty: DefTuple[bool | str] = (),
                             term_type: DefTuple[Optional[str]] = (),
                             term_size: DefTuple[TermSizeArg] = (),
                             term_modes: DefTuple[TermModesArg] = (),
                             x11_forwarding: DefTuple[Union[int, str]] = (),
                             x11_display: DefTuple[Optional[str]] = (),
                             x11_auth_path: DefTuple[Optional[str]] = (),
                             x11_single_connection: DefTuple[bool] = (),
                             encoding: DefTuple[Optional[str]] = (),
                             errors: DefTuple[str] = (),
                             window: DefTuple[int] = (),
                             max_pktsize: DefTuple[int] = ()) -> \
            tuple[SSHClientChannel, SSHClientSession]:
        """Create an SSH client session

           This method is a coroutine which can be called to create an SSH
           client session used to execute a command, start a subsystem
           such as sftp, or if no command or subsystem is specified run an
           interactive shell. Optional arguments allow terminal and
           environment information to be provided.

           By default, this class expects string data in its send and
           receive functions, which it encodes on the SSH connection in
           UTF-8 (ISO 10646) format. An optional encoding argument can
           be passed in to select a different encoding, or `None` can
           be passed in if the application wishes to send and receive
           raw bytes. When an encoding is set, an optional errors
           argument can be passed in to select what Unicode error
           handling strategy to use.

           Other optional arguments include the SSH receive window size and
           max packet size which default to 2 MB and 32 KB, respectively.

           :param session_factory:
               A `callable` which returns an :class:`SSHClientSession` object
               that will be created to handle activity on this session
           :param command: (optional)
               The remote command to execute. By default, an interactive
               shell is started if no command or subsystem is provided.
           :param subsystem: (optional)
               The name of a remote subsystem to start up.
           :param env: (optional)
               The  environment variables to set for this session. Keys and
               values passed in here will be converted to Unicode strings
               encoded as UTF-8 (ISO 10646) for transmission.

                   .. note:: Many SSH servers restrict which environment
                             variables a client is allowed to set. The
                             server's configuration may need to be edited
                             before environment variables can be
                             successfully set in the remote environment.
           :param send_env: (optional)
               A list of environment variable names to pull from
               `os.environ` and set for this session. Wildcards patterns
               using `'*'` and `'?'` are allowed, and all variables with
               matching names will be sent with whatever value is set
               in the local environment. If a variable is present in both
               env and send_env, the value from env will be used.
           :param request_pty: (optional)
               Whether or not to request a pseudo-terminal (PTY) for this
               session. This defaults to `True`, which means to request a
               PTY whenever the `term_type` is set. Other possible values
               include `False` to never request a PTY, `'force'` to always
               request a PTY even without `term_type` being set, or `'auto'`
               to request a TTY when `term_type` is set but only when
               starting an interactive shell.
           :param term_type: (optional)
               The terminal type to set for this session.
           :param term_size: (optional)
               The terminal width and height in characters and optionally
               the width and height in pixels.
           :param term_modes: (optional)
               POSIX terminal modes to set for this session, where keys are
               taken from :ref:`POSIX terminal modes <PTYModes>` with values
               defined in section 8 of :rfc:`RFC 4254 <4254#section-8>`.
           :param x11_forwarding: (optional)
               Whether or not to request X11 forwarding for this session,
               defaulting to `False`. If set to `True`, X11 forwarding will
               be requested and a failure will raise :exc:`ChannelOpenError`.
               It can also be set to `'ignore_failure'` to attempt X11
               forwarding but ignore failures.
           :param x11_display: (optional)
               The display that X11 connections should be forwarded to,
               defaulting to the value in the environment variable `DISPLAY`.
           :param x11_auth_path: (optional)
               The path to the Xauthority file to read X11 authentication
               data from, defaulting to the value in the environment variable
               `XAUTHORITY` or the file :file:`.Xauthority` in the user's
               home directory if that's not set.
           :param x11_single_connection: (optional)
               Whether or not to limit X11 forwarding to a single connection,
               defaulting to `False`.
           :param encoding: (optional)
               The Unicode encoding to use for data exchanged on this session.
           :param errors: (optional)
               The error handling strategy to apply on Unicode encode/decode
               errors.
           :param window: (optional)
               The receive window size for this session.
           :param max_pktsize: (optional)
               The maximum packet size for this session.
           :type session_factory: `callable`
           :type command: `str`
           :type subsystem: `str`
           :type env: `dict` with `bytes` or `str` keys and values
           :type send_env: `list` of `bytes` or `str`
           :type request_pty: `bool`, `'force'`, or `'auto'`
           :type term_type: `str`
           :type term_size: `tuple` of 2 or 4 `int` values
           :type term_modes: `dict` with `int` keys and values
           :type x11_forwarding: `bool` or `'ignore_failure'`
           :type x11_display: `str`
           :type x11_auth_path: `str`
           :type x11_single_connection: `bool`
           :type encoding: `str` or `None`
           :type errors: `str`
           :type window: `int`
           :type max_pktsize: `int`

           :returns: an :class:`SSHClientChannel` and :class:`SSHClientSession`

           :raises: :exc:`ChannelOpenError` if the session can't be opened

        """

        if command == ():
            command = self._options.command

        if subsystem == ():
            subsystem = self._options.subsystem

        if env == ():
            env = self._options.env

        if send_env == ():
            send_env = self._options.send_env

        if request_pty == ():
            request_pty = self._options.request_pty

        if term_type == ():
            term_type = self._options.term_type

        if term_size == ():
            term_size = self._options.term_size

        if term_modes == ():
            term_modes = self._options.term_modes

        if x11_forwarding == ():
            x11_forwarding = self._options.x11_forwarding

        if x11_display == ():
            x11_display = self._options.x11_display

        if x11_auth_path == ():
            x11_auth_path = self._options.x11_auth_path

        if x11_single_connection == ():
            x11_single_connection = self._options.x11_single_connection

        if encoding == ():
            encoding = self._options.encoding

        if errors == ():
            errors = self._options.errors

        if window == ():
            window = self._options.window

        if max_pktsize == ():
            max_pktsize = self._options.max_pktsize

        new_env: dict[bytes, bytes] = {}

        if send_env:
            new_env.update(lookup_env(send_env))

        if env:
            new_env.update(encode_env(env))

        if request_pty == 'force':
            request_pty = True
        elif request_pty == 'auto':
            request_pty = bool(term_type and not (command or subsystem))
        elif request_pty:
            request_pty = bool(term_type)

        command: Optional[str]
        subsystem: Optional[str]
        request_pty: bool
        term_type: Optional[str]
        term_size: TermSizeArg
        term_modes: TermModesArg
        x11_forwarding: Union[bool, str]
        x11_display: Optional[str]
        x11_auth_path: Optional[str]
        x11_single_connection: bool
        encoding: Optional[str]
        errors: str
        window: int
        max_pktsize: int

        chan = SSHClientChannel(self, self._loop, encoding, errors,
                                window, max_pktsize)

        session = await chan.create(session_factory, command, subsystem,
                                    new_env, request_pty, term_type, term_size,
                                    term_modes or {}, x11_forwarding,
                                    x11_display, x11_auth_path,
                                    x11_single_connection,
                                    bool(self._agent_forward_path))

        return chan, session

    async def open_session(self, *args: object, **kwargs: object) -> \
            tuple[SSHWriter, SSHReader, SSHReader]:
        """Open an SSH client session

           This method is a coroutine wrapper around :meth:`create_session`
           designed to provide a "high-level" stream interface for creating
           an SSH client session. Instead of taking a `session_factory`
           argument for constructing an object which will handle activity
           on the session via callbacks, it returns an :class:`SSHWriter`
           and two :class:`SSHReader` objects representing stdin, stdout,
           and stderr which can be used to perform I/O on the session. With
           the exception of `session_factory`, all of the arguments to
           :meth:`create_session` are supported and have the same meaning.

        """

        chan, session = await self.create_session(
            SSHClientStreamSession, *args, **kwargs) # type: ignore

        session: SSHClientStreamSession

        return (SSHWriter(session, chan), SSHReader(session, chan),
                SSHReader(session, chan, EXTENDED_DATA_STDERR))

    # pylint: disable=redefined-builtin
    @async_context_manager # type: ignore
    async def create_process(self, *args: object,
                             input: Optional[AnyStr] = None,
                             stdin: ProcessSource = PIPE,
                             stdout: ProcessTarget = PIPE,
                             stderr: ProcessTarget = PIPE,
                             bufsize: int = io.DEFAULT_BUFFER_SIZE,
                             send_eof: bool = True, recv_eof: bool = True,
                             **kwargs: object) -> SSHClientProcess[AnyStr]:
        """Create a process on the remote system

           This method is a coroutine wrapper around :meth:`create_session`
           which can be used to execute a command, start a subsystem,
           or start an interactive shell, optionally redirecting stdin,
           stdout, and stderr to and from files or pipes attached to
           other local and remote processes.

           By default, the stdin, stdout, and stderr arguments default
           to the special value `PIPE` which means that they can be
           read and written interactively via stream objects which are
           members of the :class:`SSHClientProcess` object this method
           returns. If other file-like objects are provided as arguments,
           input or output will automatically be redirected to them. The
           special value `DEVNULL` can be used to provide no input or
           discard all output, and the special value `STDOUT` can be
           provided as `stderr` to send its output to the same stream
           as `stdout`.

           In addition to the arguments below, all arguments to
           :meth:`create_session` except for `session_factory` are
           supported and have the same meaning.

           :param input: (optional)
               Input data to feed to standard input of the remote process.
               If specified, this argument takes precedence over stdin.
               Data should be a `str` if encoding is set, or `bytes` if not.
           :param stdin: (optional)
               A filename, file-like object, file descriptor, socket, or
               :class:`SSHReader` to feed to standard input of the remote
               process, or `DEVNULL` to provide no input.
           :param stdout: (optional)
               A filename, file-like object, file descriptor, socket, or
               :class:`SSHWriter` to feed standard output of the remote
               process to, or `DEVNULL` to discard this output.
           :param stderr: (optional)
               A filename, file-like object, file descriptor, socket, or
               :class:`SSHWriter` to feed standard error of the remote
               process to, `DEVNULL` to discard this output, or `STDOUT`
               to feed standard error to the same place as stdout.
           :param bufsize: (optional)
               Buffer size to use when feeding data from a file to stdin
           :param send_eof:
               Whether or not to send EOF to the channel when EOF is
               received from stdin, defaulting to `True`. If set to `False`,
               the channel will remain open after EOF is received on stdin,
               and multiple sources can be redirected to the channel.
           :param recv_eof:
               Whether or not to send EOF to stdout and stderr when EOF is
               received from the channel, defaulting to `True`. If set to
               `False`, the redirect targets of stdout and stderr will remain
               open after EOF is received on the channel and can be used for
               multiple redirects.
           :type input: `str` or `bytes`
           :type bufsize: `int`
           :type send_eof: `bool`
           :type recv_eof: `bool`

           :returns: :class:`SSHClientProcess`

           :raises: :exc:`ChannelOpenError` if the channel can't be opened

        """

        chan, process = await self.create_session(
            SSHClientProcess, *args, **kwargs) # type: ignore

        new_stdin: Optional[ProcessSource] = stdin
        process: SSHClientProcess

        if input:
            chan.write(input)
            chan.write_eof()
            new_stdin = None

        await process.redirect(new_stdin, stdout, stderr,
                               bufsize, send_eof, recv_eof)

        return process

    async def create_subprocess(self, protocol_factory: SubprocessFactory,
                                command: DefTuple[Optional[str]] = (),
                                bufsize: int = io.DEFAULT_BUFFER_SIZE,
                                input: Optional[AnyStr] = None,
                                stdin: ProcessSource = PIPE,
                                stdout: ProcessTarget = PIPE,
                                stderr: ProcessTarget = PIPE,
                                encoding: Optional[str] = None,
                                **kwargs: object) -> \
            tuple[SSHSubprocessTransport, SSHSubprocessProtocol]:
        """Create a subprocess on the remote system

           This method is a coroutine wrapper around :meth:`create_session`
           which can be used to execute a command, start a subsystem,
           or start an interactive shell, optionally redirecting stdin,
           stdout, and stderr to and from files or pipes attached to
           other local and remote processes similar to :meth:`create_process`.
           However, instead of performing interactive I/O using
           :class:`SSHReader` and :class:`SSHWriter` objects, the caller
           provides a function which returns an object which conforms
           to the :class:`asyncio.SubprocessProtocol` and this call
           returns that and an :class:`SSHSubprocessTransport` object which
           conforms to :class:`asyncio.SubprocessTransport`.

           With the exception of the addition of `protocol_factory`, all
           of the arguments are the same as :meth:`create_process`.

           :param protocol_factory:
               A `callable` which returns an :class:`SSHSubprocessProtocol`
               object that will be created to handle activity on this
               session.
           :type protocol_factory: `callable`

           :returns: an :class:`SSHSubprocessTransport` and
                     :class:`SSHSubprocessProtocol`

           :raises: :exc:`ChannelOpenError` if the channel can't be opened

        """

        def transport_factory() -> SSHSubprocessTransport:
            """Return a subprocess transport"""

            return SSHSubprocessTransport(protocol_factory)

        _, transport = await self.create_session(transport_factory, command,
                                                 encoding=encoding,
                                                 **kwargs) # type: ignore

        new_stdin: Optional[ProcessSource] = stdin
        transport: SSHSubprocessTransport

        if input:
            stdin_pipe: SSHSubprocessWritePipe = transport.get_pipe_transport(0)
            stdin_pipe.write(input)
            stdin_pipe.write_eof()
            new_stdin = None

        await transport.redirect(new_stdin, stdout, stderr, bufsize)

        return transport, transport.get_protocol()
    # pylint: enable=redefined-builtin

    async def run(self, *args: object, check: bool = False,
                  timeout: Optional[float] = None,
                  **kwargs: object) -> SSHCompletedProcess:
        """Run a command on the remote system and collect its output

           This method is a coroutine wrapper around :meth:`create_process`
           which can be used to run a process to completion when no
           interactivity is needed. All of the arguments to
           :meth:`create_process` can be passed in to provide input or
           redirect stdin, stdout, and stderr, but this method waits until
           the process exits and returns an :class:`SSHCompletedProcess`
           object with the exit status or signal information and the
           output to stdout and stderr (if not redirected).

           If the check argument is set to `True`, a non-zero exit status
           from the remote process will trigger the :exc:`ProcessError`
           exception to be raised.

           In addition to the argument below, all arguments to
           :meth:`create_process` are supported and have the same meaning.

           If a timeout is specified and it expires before the process
           exits, the :exc:`TimeoutError` exception will be raised. By
           default, no timeout is set and this call will wait indefinitely.

           :param check: (optional)
               Whether or not to raise :exc:`ProcessError` when a non-zero
               exit status is returned
           :param timeout:
               Amount of time in seconds to wait for process to exit or
               `None` to wait indefinitely
           :type check: `bool`
           :type timeout: `int`, `float`, or `None`

           :returns: :class:`SSHCompletedProcess`

           :raises: | :exc:`ChannelOpenError` if the session can't be opened
                    | :exc:`ProcessError` if checking non-zero exit status
                    | :exc:`TimeoutError` if the timeout expires before exit

        """

        process = await self.create_process(*args, **kwargs) # type: ignore

        return await process.wait(check, timeout)

    async def create_connection(
            self, session_factory: SSHTCPSessionFactory[AnyStr],
            remote_host: str, remote_port: int, orig_host: str = '',
            orig_port: int = 0, *, encoding: Optional[str] = None,
            errors: str = 'strict', window: int = DEFAULT_WINDOW,
            max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> \
                tuple[SSHTCPChannel[AnyStr], SSHTCPSession[AnyStr]]:
        """Create an SSH TCP direct connection

           This method is a coroutine which can be called to request that
           the server open a new outbound TCP connection to the specified
           destination host and port. If the connection is successfully
           opened, a new SSH channel will be opened with data being handled
           by a :class:`SSHTCPSession` object created by `session_factory`.

           Optional arguments include the host and port of the original
           client opening the connection when performing TCP port forwarding.

           By default, this class expects data to be sent and received as
           raw bytes. However, an optional encoding argument can be passed
           in to select the encoding to use, allowing the application send
           and receive string data. When encoding is set, an optional errors
           argument can be passed in to select what Unicode error handling
           strategy to use.

           Other optional arguments include the SSH receive window size and
           max packet size which default to 2 MB and 32 KB, respectively.

           :param session_factory:
               A `callable` which returns an :class:`SSHTCPSession` object
               that will be created to handle activity on this session
           :param remote_host:
               The remote hostname or address to connect to
           :param remote_port:
               The remote port number to connect to
           :param orig_host: (optional)
               The hostname or address of the client requesting the connection
           :param orig_port: (optional)
               The port number of the client requesting the connection
           :param encoding: (optional)
               The Unicode encoding to use for data exchanged on the connection
           :param errors: (optional)
               The error handling strategy to apply on encode/decode errors
           :param window: (optional)
               The receive window size for this session
           :param max_pktsize: (optional)
               The maximum packet size for this session
           :type session_factory: `callable`
           :type remote_host: `str`
           :type remote_port: `int`
           :type orig_host: `str`
           :type orig_port: `int`
           :type encoding: `str` or `None`
           :type errors: `str`
           :type window: `int`
           :type max_pktsize: `int`

           :returns: an :class:`SSHTCPChannel` and :class:`SSHTCPSession`

           :raises: :exc:`ChannelOpenError` if the connection can't be opened

        """

        self.logger.info('Opening direct TCP connection to %s',
                         (remote_host, remote_port))
        self.logger.info('  Client address: %s', (orig_host, orig_port))

        chan = self.create_tcp_channel(encoding, errors, window, max_pktsize)

        session = await chan.connect(session_factory, remote_host, remote_port,
                                     orig_host, orig_port)

        return chan, session

    async def open_connection(self, *args: object, **kwargs: object) -> \
            tuple[SSHReader, SSHWriter]:
        """Open an SSH TCP direct connection

           This method is a coroutine wrapper around :meth:`create_connection`
           designed to provide a "high-level" stream interface for creating
           an SSH TCP direct connection. Instead of taking a
           `session_factory` argument for constructing an object which will
           handle activity on the session via callbacks, it returns
           :class:`SSHReader` and :class:`SSHWriter` objects which can be
           used to perform I/O on the connection.

           With the exception of `session_factory`, all of the arguments
           to :meth:`create_connection` are supported and have the same
           meaning here.

           :returns: an :class:`SSHReader` and :class:`SSHWriter`

           :raises: :exc:`ChannelOpenError` if the connection can't be opened

        """

        chan, session = await self.create_connection(
            SSHTCPStreamSession, *args, **kwargs) # type: ignore

        session: SSHTCPStreamSession

        return SSHReader(session, chan), SSHWriter(session, chan)


    async def create_unix_connection(
            self, session_factory: SSHUNIXSessionFactory[AnyStr],
            remote_path: str, *, encoding: Optional[str] = None,
            errors: str = 'strict', window: int = DEFAULT_WINDOW,
            max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> \
                tuple[SSHUNIXChannel[AnyStr], SSHUNIXSession[AnyStr]]:
        """Create an SSH UNIX domain socket direct connection

           This method is a coroutine which can be called to request that
           the server open a new outbound UNIX domain socket connection to
           the specified destination path. If the connection is successfully
           opened, a new SSH channel will be opened with data being handled
           by a :class:`SSHUNIXSession` object created by `session_factory`.

           By default, this class expects data to be sent and received as
           raw bytes. However, an optional encoding argument can be passed
           in to select the encoding to use, allowing the application to
           send and receive string data. When encoding is set, an optional
           errors argument can be passed in to select what Unicode error
           handling strategy to use.

           Other optional arguments include the SSH receive window size and
           max packet size which default to 2 MB and 32 KB, respectively.

           :param session_factory:
               A `callable` which returns an :class:`SSHUNIXSession` object
               that will be created to handle activity on this session
           :param remote_path:
               The remote path to connect to
           :param encoding: (optional)
               The Unicode encoding to use for data exchanged on the connection
           :param errors: (optional)
               The error handling strategy to apply on encode/decode errors
           :param window: (optional)
               The receive window size for this session
           :param max_pktsize: (optional)
               The maximum packet size for this session
           :type session_factory: `callable`
           :type remote_path: `str`
           :type encoding: `str` or `None`
           :type errors: `str`
           :type window: `int`
           :type max_pktsize: `int`

           :returns: an :class:`SSHUNIXChannel` and :class:`SSHUNIXSession`

           :raises: :exc:`ChannelOpenError` if the connection can't be opened

        """

        chan = self.create_unix_channel(encoding, errors, window, max_pktsize)

        session = await chan.connect(session_factory, remote_path)

        return chan, session

    async def open_unix_connection(self, *args: object, **kwargs: object) -> \
            tuple[SSHReader, SSHWriter]:
        """Open an SSH UNIX domain socket direct connection

           This method is a coroutine wrapper around
           :meth:`create_unix_connection` designed to provide a "high-level"
           stream interface for creating an SSH UNIX domain socket direct
           connection. Instead of taking a `session_factory` argument for
           constructing an object which will handle activity on the session
           via callbacks, it returns :class:`SSHReader` and :class:`SSHWriter`
           objects which can be used to perform I/O on the connection.

           With the exception of `session_factory`, all of the arguments
           to :meth:`create_unix_connection` are supported and have the same
           meaning here.

           :returns: an :class:`SSHReader` and :class:`SSHWriter`

           :raises: :exc:`ChannelOpenError` if the connection can't be opened

        """

        chan, session = \
            await self.create_unix_connection(SSHUNIXStreamSession,
                                              *args, **kwargs) # type: ignore

        session: SSHUNIXStreamSession

        return SSHReader(session, chan), SSHWriter(session, chan)

    async def create_ssh_connection(self, client_factory: _ClientFactory,
                                    host: str, port: DefTuple[int] = (),
                                    **kwargs: object) -> \
                tuple['SSHClientConnection', SSHClient]:
        """Create a tunneled SSH client connection

           This method is a coroutine which can be called to open an
           SSH client connection to the requested host and port tunneled
           inside this already established connection. It takes all the
           same arguments as :func:`create_connection` but requests
           that the upstream SSH server open the connection rather than
           connecting directly.

        """

        return await create_connection(client_factory, host, port,
                                       tunnel=self, **kwargs) # type: ignore

    @async_context_manager
    async def connect_ssh(self, host: str, port: DefTuple[int] = (),
                          **kwargs: object) -> 'SSHClientConnection':
        """Make a tunneled SSH client connection

           This method is a coroutine which can be called to open an
           SSH client connection to the requested host and port tunneled
           inside this already established connection. It takes all the
           same arguments as :func:`connect` but requests that the upstream
           SSH server open the connection rather than connecting directly.

        """

        return await connect(host, port, tunnel=self, **kwargs) # type: ignore

    @async_context_manager
    async def listen_ssh(self, host: str = '', port: DefTuple[int] = (),
                         **kwargs: object) -> SSHAcceptor:
        """Create a tunneled SSH listener

           This method is a coroutine which can be called to open a remote
           SSH listener on the requested host and port tunneled inside this
           already established connection. It takes all the same arguments as
           :func:`listen` but requests that the upstream SSH server open the
           listener rather than listening directly via TCP/IP.

        """

        return await listen(host, port, tunnel=self, **kwargs) # type: ignore

    @async_context_manager
    async def listen_reverse_ssh(self, host: str = '',
                                 port: DefTuple[int] = (),
                                 **kwargs: object) -> SSHAcceptor:
        """Create a tunneled reverse direction SSH listener

           This method is a coroutine which can be called to open a remote
           SSH listener on the requested host and port tunneled inside this
           already established connection. It takes all the same arguments as
           :func:`listen_reverse` but requests that the upstream SSH server
           open the listener rather than listening directly via TCP/IP.

        """

        return await listen_reverse(host, port, tunnel=self,
                                    **kwargs) # type: ignore

    async def create_tun(
            self, session_factory: SSHTunTapSessionFactory,
            remote_unit: Optional[int] = None, *, window: int = DEFAULT_WINDOW,
            max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> \
                tuple[SSHTunTapChannel, SSHTunTapSession]:
        """Create an SSH layer 3 tunnel

           This method is a coroutine which can be called to request that
           the server open a new outbound layer 3 tunnel to the specified
           remote TUN device. If the tunnel is successfully opened, a new
           SSH channel will be opened with data being handled by a
           :class:`SSHTunTapSession` object created by `session_factory`.

           Optional arguments include the SSH receive window size and max
           packet size which default to 2 MB and 32 KB, respectively.

           :param session_factory:
               A `callable` which returns an :class:`SSHUNIXSession` object
               that will be created to handle activity on this session
           :param remote_unit:
               The remote TUN device to connect to
           :param window: (optional)
               The receive window size for this session
           :param max_pktsize: (optional)
               The maximum packet size for this session
           :type session_factory: `callable`
           :type remote_unit: `int` or `None`
           :type window: `int`
           :type max_pktsize: `int`

           :returns: an :class:`SSHTunTapChannel` and :class:`SSHTunTapSession`

           :raises: :exc:`ChannelOpenError` if the connection can't be opened

        """

        chan = self.create_tuntap_channel(window, max_pktsize)

        session = await chan.open(session_factory, SSH_TUN_MODE_POINTTOPOINT,
                                  remote_unit)

        return chan, session

    async def create_tap(
            self, session_factory: SSHTunTapSessionFactory,
            remote_unit: Optional[int] = None, *, window: int = DEFAULT_WINDOW,
            max_pktsize: int = DEFAULT_MAX_PKTSIZE) -> \
                tuple[SSHTunTapChannel, SSHTunTapSession]:
        """Create an SSH layer 2 tunnel

           This method is a coroutine which can be called to request that
           the server open a new outbound layer 2 tunnel to the specified
           remote TAP device. If the tunnel is successfully opened, a new
           SSH channel will be opened with data being handled by a
           :class:`SSHTunTapSession` object created by `session_factory`.

           Optional arguments include the SSH receive window size and max
           packet size which default to 2 MB and 32 KB, respectively.

           :param session_factory:
               A `callable` which returns an :class:`SSHUNIXSession` object
               that will be created to handle activity on this session
           :param remote_unit:
               The remote TAP device to connect to
           :param window: (optional)
               The receive window size for this session
           :param max_pktsize: (optional)
               The maximum packet size for this session
           :type session_factory: `callable`
           :type remote_unit: `int` or `None`
           :type window: `int`
           :type max_pktsize: `int`

           :returns: an :class:`SSHTunTapChannel` and :class:`SSHTunTapSession`

           :raises: :exc:`ChannelOpenError` if the connection can't be opened

        """

        self.logger.info('Opening layer 2 tunnel to remote unit %s',
                         'any' if remote_unit is None else str(remote_unit))

        chan = self.create_tuntap_channel(window, max_pktsize)

        session = await chan.open(session_factory, SSH_TUN_MODE_ETHERNET,
                                  remote_unit)

        return chan, session

    async def open_tun(self, *args: object, **kwargs: object) -> \
            tuple[SSHReader, SSHWriter]:
        """Open an SSH layer 3 tunnel

           This method is a coroutine wrapper around :meth:`create_tun`
           designed to provide a "high-level" stream interface for creating
           an SSH layer 3 tunnel. Instead of taking a `session_factory`
           argument for constructing an object which will handle activity
           on the session via callbacks, it returns :class:`SSHReader` and
           :class:`SSHWriter` objects which can be used to perform I/O on
           the tunnel.

           With the exception of `session_factory`, all of the arguments
           to :meth:`create_tun` are supported and have the same meaning here.

           :returns: an :class:`SSHReader` and :class:`SSHWriter`

           :raises: :exc:`ChannelOpenError` if the connection can't be opened

        """

        chan, session = await self.create_tun(SSHTunTapStreamSession,
                                              *args, **kwargs) # type: ignore

        session: SSHTunTapStreamSession

        return SSHReader(session, chan), SSHWriter(session, chan)

    async def open_tap(self, *args: object, **kwargs: object) -> \
            tuple[SSHReader, SSHWriter]:
        """Open an SSH layer 2 tunnel

           This method is a coroutine wrapper around :meth:`create_tap`
           designed to provide a "high-level" stream interface for creating
           an SSH layer 2 tunnel. Instead of taking a `session_factory`
           argument for constructing an object which will handle activity
           on the session via callbacks, it returns :class:`SSHReader` and
           :class:`SSHWriter` objects which can be used to perform I/O on
           the tunnel.

           With the exception of `session_factory`, all of the arguments
           to :meth:`create_tap` are supported and have the same meaning here.

           :returns: an :class:`SSHReader` and :class:`SSHWriter`

           :raises: :exc:`ChannelOpenError` if the connection can't be opened

        """

        chan, session = await self.create_tap(*args, **kwargs) # type: ignore

        return SSHReader(session, chan), SSHWriter(session, chan)

    @async_context_manager
    async def forward_local_port_to_path(
            self, listen_host: str, listen_port: int, dest_path: str,
            accept_handler: Optional[SSHAcceptHandler] = None) -> SSHListener:
        """Set up local TCP port forwarding to a remote UNIX domain socket

           This method is a coroutine which attempts to set up port
           forwarding from a local TCP listening port to a remote UNIX
           domain path via the SSH connection. If the request is successful,
           the return value is an :class:`SSHListener` object which can be
           used later to shut down the port forwarding.

           :param listen_host:
               The hostname or address on the local host to listen on
           :param listen_port:
               The port number on the local host to listen on
           :param dest_path:
               The path on the remote host to forward the connections to
           :param accept_handler:
               A `callable` or coroutine which takes arguments of the
               original host and port of the client and decides whether
               or not to allow connection forwarding, returning `True` to
               accept the connection and begin forwarding or `False` to
               reject and close it.
           :type listen_host: `str`
           :type listen_port: `int`
           :type dest_path: `str`
           :type accept_handler: `callable` or coroutine

           :returns: :class:`SSHListener`

           :raises: :exc:`OSError` if the listener can't be opened

        """

        async def tunnel_connection(
                session_factory: SSHUNIXSessionFactory[bytes],
                orig_host: str, orig_port: int) -> \
                    tuple[SSHUNIXChannel[bytes], SSHUNIXSession[bytes]]:
            """Forward a local connection over SSH"""

            if accept_handler:
                result = accept_handler(orig_host, orig_port)

                if inspect.isawaitable(result):
                    result: bool = await result

                if not result:
                    raise ChannelOpenError(OPEN_ADMINISTRATIVELY_PROHIBITED,
                                           'Connection forwarding denied')

            return await self.create_unix_connection(session_factory, dest_path)

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

        self._local_listeners[listen_host, listen_port] = listener

        return listener

    @async_context_manager
    async def forward_local_path_to_port(self, listen_path: str,
                                         dest_host: str,
                                         dest_port: int) -> SSHListener:
        """Set up local UNIX domain socket forwarding to a remote TCP port

           This method is a coroutine which attempts to set up UNIX domain
           socket forwarding from a local listening path to a remote host
           and port via the SSH connection. If the request is successful,
           the return value is an :class:`SSHListener` object which can
           be used later to shut down the UNIX domain socket forwarding.

           :param listen_path:
               The path on the local host to listen on
           :param dest_host:
               The hostname or address to forward the connections to
           :param dest_port:
               The port number to forward the connections to
           :type listen_path: `str`
           :type dest_host: `str`
           :type dest_port: `int`

           :returns: :class:`SSHListener`

           :raises: :exc:`OSError` if the listener can't be opened

        """

        async def tunnel_connection(
                session_factory: SSHTCPSessionFactory[bytes]) -> \
                    tuple[SSHTCPChannel[bytes], SSHTCPSession[bytes]]:
            """Forward a local connection over SSH"""

            return await self.create_connection(session_factory, dest_host,
                                                dest_port, '', 0)

        self.logger.info('Creating local UNIX forwarder from %s to %s',
                         listen_path, (dest_host, dest_port))

        try:
            listener = await create_unix_forward_listener(self, self._loop,
                                                          tunnel_connection,
                                                          listen_path)
        except OSError as exc:
            raise

        self._local_listeners[listen_path] = listener

        return listener

    @async_context_manager
    async def forward_remote_port(self, listen_host: str,
                                  listen_port: int, dest_host: str,
                                  dest_port: int) -> SSHListener:
        """Set up remote port forwarding

           This method is a coroutine which attempts to set up port
           forwarding from a remote listening port to a local host and port
           via the SSH connection. If the request is successful, the
           return value is an :class:`SSHListener` object which can be
           used later to shut down the port forwarding. If the request
           fails, `None` is returned.

           :param listen_host:
               The hostname or address on the remote host to listen on
           :param listen_port:
               The port number on the remote host to listen on
           :param dest_host:
               The hostname or address to forward connections to
           :param dest_port:
               The port number to forward connections to
           :type listen_host: `str`
           :type listen_port: `int`
           :type dest_host: `str`
           :type dest_port: `int`

           :returns: :class:`SSHListener`

           :raises: :class:`ChannelListenError` if the listener can't be opened

        """

        def session_factory(_orig_host: str,
                            _orig_port: int) -> Awaitable[SSHTCPSession]:
            """Return an SSHTCPSession used to do remote port forwarding"""

            return self.forward_connection(dest_host, dest_port)

        self.logger.info('Creating remote TCP forwarder from %s to %s',
                         (listen_host, listen_port), (dest_host, dest_port))

        return await self.create_server(session_factory, listen_host,
                                        listen_port)

    @async_context_manager
    async def forward_socks(self, listen_host: str,
                            listen_port: int) -> SSHListener:
        """Set up local port forwarding via SOCKS

           This method is a coroutine which attempts to set up dynamic
           port forwarding via SOCKS on the specified local host and
           port. Each SOCKS request contains the destination host and
           port to connect to and triggers a request to tunnel traffic
           to the requested host and port via the SSH connection.

           If the request is successful, the return value is an
           :class:`SSHListener` object which can be used later to shut
           down the port forwarding.

           :param listen_host:
               The hostname or address on the local host to listen on
           :param listen_port:
               The port number on the local host to listen on
           :type listen_host: `str`
           :type listen_port: `int`

           :returns: :class:`SSHListener`

           :raises: :exc:`OSError` if the listener can't be opened

        """

        async def tunnel_socks(session_factory: SSHTCPSessionFactory[bytes],
                               dest_host: str, dest_port: int,
                               orig_host: str, orig_port: int) -> \
                tuple[SSHTCPChannel[bytes], SSHTCPSession[bytes]]:
            """Forward a local SOCKS connection over SSH"""

            return await self.create_connection(session_factory,
                                                dest_host, dest_port,
                                                orig_host, orig_port)

        try:
            listener = await create_socks_listener(self, self._loop,
                                                   tunnel_socks,
                                                   listen_host, listen_port)
        except OSError as exc:
            raise

        if listen_port == 0:
            listen_port = listener.get_port()

        self._local_listeners[listen_host, listen_port] = listener

        return listener

    @async_context_manager
    async def forward_tun(self, local_unit: Optional[int] = None,
                          remote_unit: Optional[int] = None) -> SSHForwarder:
        """Set up layer 3 forwarding

           This method is a coroutine which attempts to set up layer 3
           packet forwarding between local and remote TUN devices. If the
           request is successful, the return value is an :class:`SSHForwarder`
           object which can be used later to shut down the forwarding.

           :param local_unit:
               The unit number of the local TUN device to use
           :param remote_unit:
               The unit number of the remote TUN device to use
           :type local_unit: `int` or `None`
           :type remote_unit: `int` or `None`

           :returns: :class:`SSHForwarder`

           :raises: | :exc:`OSError` if the local TUN device can't be opened
                    | :exc:`ChannelOpenError` if the SSH channel can't be opened

        """

        def session_factory() -> SSHTunTapSession:
            """Return an SSHTunTapSession used to do layer 3 forwarding"""

            return self.forward_tuntap(SSH_TUN_MODE_POINTTOPOINT,
                                            local_unit)

        _, peer = await self.create_tun(session_factory, remote_unit)

        return peer

    @async_context_manager
    async def forward_tap(self, local_unit: Optional[int] = None,
                          remote_unit: Optional[int] = None) -> SSHForwarder:
        """Set up layer 2 forwarding

           This method is a coroutine which attempts to set up layer 2
           packet forwarding between local and remote TAP devices. If the
           request is successful, the return value is an :class:`SSHForwarder`
           object which can be used later to shut down the forwarding.

           :param local_unit:
               The unit number of the local TAP device to use
           :param remote_unit:
               The unit number of the remote TAP device to use
           :type local_unit: `int` or `None`
           :type remote_unit: `int` or `None`

           :returns: :class:`SSHForwarder`

           :raises: | :exc:`OSError` if the local TUN device can't be opened
                    | :exc:`ChannelOpenError` if the SSH channel can't be opened

        """

        def session_factory() -> SSHTunTapSession:
            """Return an SSHTunTapSession used to do layer 2 forwarding"""

            return self.forward_tuntap(SSH_TUN_MODE_ETHERNET, local_unit)
        
        _, peer = await self.create_tap(session_factory, remote_unit)

        return peer

    @async_context_manager
    async def start_sftp_client(self, env: DefTuple[Optional[Env]] = (),
                                send_env: DefTuple[Optional[EnvSeq]] = (),
                                path_encoding: Optional[str] = 'utf-8',
                                path_errors = 'strict',
                                sftp_version = MIN_SFTP_VERSION) -> SFTPClient:
        """Start an SFTP client

           This method is a coroutine which attempts to start a secure
           file transfer session. If it succeeds, it returns an
           :class:`SFTPClient` object which can be used to copy and
           access files on the remote host.

           An optional Unicode encoding can be specified for sending and
           receiving pathnames, defaulting to UTF-8 with strict error
           checking. If an encoding of `None` is specified, pathnames
           will be left as bytes rather than being converted to & from
           strings.

           :param env: (optional)
               The environment variables to set for this SFTP session. Keys
               and values passed in here will be converted to Unicode
               strings encoded as UTF-8 (ISO 10646) for transmission.

                   .. note:: Many SSH servers restrict which environment
                             variables a client is allowed to set. The
                             server's configuration may need to be edited
                             before environment variables can be
                             successfully set in the remote environment.
           :param send_env: (optional)
               A list of environment variable names to pull from
               `os.environ` and set for this SFTP session. Wildcards
               patterns using `'*'` and `'?'` are allowed, and all variables
               with matching names will be sent with whatever value is set
               in the local environment. If a variable is present in both
               env and send_env, the value from env will be used.
           :param path_encoding:
               The Unicode encoding to apply when sending and receiving
               remote pathnames
           :param path_errors:
               The error handling strategy to apply on encode/decode errors
           :param sftp_version: (optional)
               The maximum version of the SFTP protocol to support, currently
               either 3 or 4, defaulting to 3.
           :type env: `dict` with `str` keys and values
           :type send_env: `list` of `str`
           :type path_encoding: `str` or `None`
           :type path_errors: `str`
           :type sftp_version: `int`

           :returns: :class:`SFTPClient`

           :raises: :exc:`SFTPError` if the session can't be opened

        """

        writer, reader, _ = await self.open_session(subsystem='sftp',
                                                    env=env, send_env=send_env,
                                                    encoding=None)

        return await start_sftp_client(self, self._loop, reader, writer,
                                       path_encoding, path_errors,
                                       sftp_version)

