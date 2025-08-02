# Copyright (c) 2013-2025 by Ron Frederick <ronf@timeheart.net> and others.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License v2.0 which accompanies this
# distribution and is available at:
#
#     http://www.eclipse.org/legal/epl-2.0/
#
# This program may also be made available under the following secondary
# licenses when the conditions for such availability set forth in the
# Eclipse Public License v2.0 are satisfied:
#
#    GNU General Public License, Version 2.0, or any later versions of
#    that license
#
# SPDX-License-Identifier: EPL-2.0 OR GPL-2.0-or-later
#
# Contributors:
#     Ron Frederick - initial implementation, API, and documentation

"""SSH authentication handlers"""

from typing import (
    TYPE_CHECKING,
    Awaitable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from .gss import GSSBase, GSSError
from .misc import ProtocolError, get_symbol_names
from .misc import run_in_executor
from .packet import Boolean, String, UInt32, SSHPacket, SSHPacketHandler
from .public_key import SigningKey


if TYPE_CHECKING:
    import asyncio

    # pylint: disable=cyclic-import
    from .connection import SSHConnection, SSHClientConnection


KbdIntPrompts = Sequence[Tuple[str, bool]]
KbdIntNewChallenge = Tuple[str, str, str, KbdIntPrompts]
KbdIntChallenge = Union[bool, KbdIntNewChallenge]
KbdIntResponse = Sequence[str]

PasswordChangeResponse = Tuple[str, str]


# SSH message values for GSS auth
MSG_USERAUTH_GSSAPI_RESPONSE          = 60
MSG_USERAUTH_GSSAPI_TOKEN             = 61
MSG_USERAUTH_GSSAPI_EXCHANGE_COMPLETE = 63
MSG_USERAUTH_GSSAPI_ERROR             = 64
MSG_USERAUTH_GSSAPI_ERRTOK            = 65
MSG_USERAUTH_GSSAPI_MIC               = 66

# SSH message values for public key auth
MSG_USERAUTH_PK_OK                    = 60

# SSH message values for keyboard-interactive auth
MSG_USERAUTH_INFO_REQUEST             = 60
MSG_USERAUTH_INFO_RESPONSE            = 61

# SSH message values for password auth
MSG_USERAUTH_PASSWD_CHANGEREQ         = 60

_auth_methods: List[bytes] = []
_client_auth_handlers: Dict[bytes, Type['ClientAuth']] = {}


class Auth(SSHPacketHandler):
    """Parent class for authentication"""

    def __init__(self, conn: 'SSHConnection', coro: Awaitable[None]):
        self._conn = conn
        self._coro: Optional['asyncio.Task[None]'] = conn.create_task(coro)

    def send_packet(self, pkttype: int, *args: bytes,
                    trivial: bool = True) -> None:
        """Send an auth packet"""

        self._conn.send_userauth_packet(pkttype, *args, handler=self,
                                        trivial=trivial)
        
    def create_task(self, coro: Awaitable[None]) -> None:
        """Create an asynchronous auth task"""

        self.cancel()
        self._coro = self._conn.create_task(coro)

    def cancel(self) -> None:
        """Cancel any authentication in progress"""

        if self._coro: # pragma: no branch
            self._coro.cancel()
            self._coro = None


class ClientAuth(Auth):
    """Parent class for client authentication"""

    _conn: 'SSHClientConnection'

    def __init__(self, conn: 'SSHClientConnection', method: bytes):
        self._method = method

        super().__init__(conn, self._start())

    async def _start(self) -> None:
        """Abstract method for starting client authentication"""

        # Provided by subclass
        raise NotImplementedError

    def auth_succeeded(self) -> None:
        """Callback when auth succeeds"""

    def auth_failed(self) -> None:
        """Callback when auth fails"""

    async def send_request(self, *args: bytes,
                           key: Optional[SigningKey] = None,
                           trivial: bool = True) -> None:
        """Send a user authentication request"""

        await self._conn.send_userauth_request(self._method, *args, key=key,
                                               trivial=trivial)


class _ClientNullAuth(ClientAuth):
    """Client side implementation of null auth"""

    async def _start(self) -> None:
        """Start client null authentication"""

        await self.send_request()


class _ClientGSSKexAuth(ClientAuth):
    """Client side implementation of GSS key exchange auth"""

    async def _start(self) -> None:
        """Start client GSS key exchange authentication"""

        if self._conn.gss_kex_auth_requested():
            await self.send_request(key=self._conn.get_gss_context(),
                                    trivial=False)
        else:
            self._conn.try_next_auth(next_method=True)


class _ClientGSSMICAuth(ClientAuth):
    """Client side implementation of GSS MIC auth"""

    _handler_names = get_symbol_names(globals(), 'MSG_USERAUTH_GSSAPI_')

    def __init__(self, conn: 'SSHClientConnection', method: bytes):
        super().__init__(conn, method)

        self._gss: Optional[GSSBase] = None
        self._got_error = False

    async def _start(self) -> None:
        """Start client GSS MIC authentication"""

        if self._conn.gss_mic_auth_requested():
            self._gss = self._conn.get_gss_context()
            self._gss.reset()
            mechs = b''.join(String(mech) for mech in self._gss.mechs)
            await self.send_request(UInt32(len(self._gss.mechs)), mechs)
        else:
            self._conn.try_next_auth(next_method=True)

    def _finish(self) -> None:
        """Finish client GSS MIC authentication"""

        assert self._gss is not None

        if self._gss.provides_integrity:
            data = self._conn.get_userauth_request_data(self._method)

            self.send_packet(MSG_USERAUTH_GSSAPI_MIC,
                             String(self._gss.sign(data)),
                             trivial=False)
        else:
            self.send_packet(MSG_USERAUTH_GSSAPI_EXCHANGE_COMPLETE)

    async def _process_response(self, _pkttype: int, _pktid: int,
                                packet: SSHPacket) -> None:
        """Process a GSS response from the server"""

        mech = packet.get_string()
        packet.check_end()

        assert self._gss is not None

        if mech not in self._gss.mechs:
            raise ProtocolError('Mechanism mismatch')

        try:
            token = await run_in_executor(self._gss.step)
            assert token is not None

            self.send_packet(MSG_USERAUTH_GSSAPI_TOKEN, String(token))

            if self._gss.complete:
                self._finish()
        except GSSError as exc:
            if exc.token:
                self.send_packet(MSG_USERAUTH_GSSAPI_ERRTOK, String(exc.token))

            self._conn.try_next_auth(next_method=True)

    async def _process_token(self, _pkttype: int, _pktid: int,
                             packet: SSHPacket) -> None:
        """Process a GSS token from the server"""

        token: Optional[bytes] = packet.get_string()
        packet.check_end()

        assert self._gss is not None

        try:
            token = await run_in_executor(self._gss.step, token)

            if token:
                self.send_packet(MSG_USERAUTH_GSSAPI_TOKEN, String(token))

            if self._gss.complete:
                self._finish()
        except GSSError as exc:
            if exc.token:
                self.send_packet(MSG_USERAUTH_GSSAPI_ERRTOK, String(exc.token))

            self._conn.try_next_auth(next_method=True)

    def _process_error(self, _pkttype: int, _pktid: int,
                       packet: SSHPacket) -> None:
        """Process a GSS error from the server"""

        _ = packet.get_uint32()         # major_status
        _ = packet.get_uint32()         # minor_status
        msg = packet.get_string()
        _ = packet.get_string()         # lang
        packet.check_end()

        self._got_error = True

    async def _process_error_token(self, _pkttype: int, _pktid: int,
                                   packet: SSHPacket) -> None:
        """Process a GSS error token from the server"""

        token = packet.get_string()
        packet.check_end()

        assert self._gss is not None

        try:
            await run_in_executor(self._gss.step, token)
        except GSSError as exc:
            pass

    _packet_handlers = {
        MSG_USERAUTH_GSSAPI_RESPONSE: _process_response,
        MSG_USERAUTH_GSSAPI_TOKEN:    _process_token,
        MSG_USERAUTH_GSSAPI_ERROR:    _process_error,
        MSG_USERAUTH_GSSAPI_ERRTOK:   _process_error_token
    }


class _ClientHostBasedAuth(ClientAuth):
    """Client side implementation of host based auth"""

    async def _start(self) -> None:
        """Start client host based authentication"""

        keypair, client_host, client_username = \
            await self._conn.host_based_auth_requested()

        if keypair is None:
            self._conn.try_next_auth(next_method=True)
            return

        try:
            await self.send_request(String(keypair.algorithm),
                                    String(keypair.public_data),
                                    String(client_host),
                                    String(client_username), key=keypair)
        except ValueError:
            self._conn.try_next_auth()


class _ClientPublicKeyAuth(ClientAuth):
    """Client side implementation of public key auth"""

    _handler_names = get_symbol_names(globals(), 'MSG_USERAUTH_PK_')

    async def _start(self) -> None:
        """Start client public key authentication"""

        self._keypair = await self._conn.public_key_auth_requested()

        if self._keypair is None:
            self._conn.try_next_auth(next_method=True)
            return

        await self.send_request(Boolean(False),
                                String(self._keypair.algorithm),
                                String(self._keypair.public_data))

    async def _send_signed_request(self) -> None:
        """Send signed public key request"""

        assert self._keypair is not None

        try:
            await self.send_request(Boolean(True),
                                    String(self._keypair.algorithm),
                                    String(self._keypair.public_data),
                                    key=self._keypair, trivial=False)
        except ValueError:
            self._conn.try_next_auth()

    def _process_public_key_ok(self, _pkttype: int, _pktid: int,
                               packet: SSHPacket) -> None:
        """Process a public key ok response"""

        algorithm = packet.get_string()
        key_data = packet.get_string()
        packet.check_end()

        assert self._keypair is not None

        if (algorithm != self._keypair.algorithm or
                key_data != self._keypair.public_data):
            raise ProtocolError('Key mismatch')

        self.create_task(self._send_signed_request())

    _packet_handlers = {
        MSG_USERAUTH_PK_OK: _process_public_key_ok
    }


class _ClientKbdIntAuth(ClientAuth):
    """Client side implementation of keyboard-interactive auth"""

    _handler_names = get_symbol_names(globals(), 'MSG_USERAUTH_INFO_')

    async def _start(self) -> None:
        """Start client keyboard interactive authentication"""

        submethods = await self._conn.kbdint_auth_requested()

        if submethods is None:
            self._conn.try_next_auth(next_method=True)
            return

        await self.send_request(String(''), String(submethods))

    async def _receive_challenge(self, name: str, instruction: str, lang: str,
                                 prompts: KbdIntPrompts) -> None:
        """Receive and respond to a keyboard interactive challenge"""

        responses = \
            await self._conn.kbdint_challenge_received(name, instruction,
                                                       lang, prompts)

        if responses is None:
            self._conn.try_next_auth(next_method=True)
            return

        self.send_packet(MSG_USERAUTH_INFO_RESPONSE, UInt32(len(responses)),
                         b''.join(String(r) for r in responses),
                         trivial=not responses)

    def _process_info_request(self, _pkttype: int, _pktid: int,
                              packet: SSHPacket) -> None:
        """Process a keyboard interactive authentication request"""

        name_bytes = packet.get_string()
        instruction_bytes = packet.get_string()
        lang_bytes = packet.get_string()

        try:
            name = name_bytes.decode('utf-8')
            instruction = instruction_bytes.decode('utf-8')
            lang = lang_bytes.decode('ascii')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid keyboard interactive '
                                'info request') from None

        num_prompts = packet.get_uint32()
        prompts = []
        for _ in range(num_prompts):
            prompt_bytes = packet.get_string()
            echo = packet.get_boolean()

            try:
                prompt = prompt_bytes.decode('utf-8')
            except UnicodeDecodeError:
                raise ProtocolError('Invalid keyboard interactive '
                                    'info request') from None

            prompts.append((prompt, echo))

        self.create_task(self._receive_challenge(name, instruction,
                                                 lang, prompts))

    _packet_handlers = {
        MSG_USERAUTH_INFO_REQUEST: _process_info_request
    }


class _ClientPasswordAuth(ClientAuth):
    """Client side implementation of password auth"""

    _handler_names = get_symbol_names(globals(), 'MSG_USERAUTH_PASSWD_')

    def __init__(self, conn: 'SSHClientConnection', method: bytes):
        super().__init__(conn, method)

        self._password_change = False

    async def _start(self) -> None:
        """Start client password authentication"""

        password = await self._conn.password_auth_requested()

        if password is None:
            self._conn.try_next_auth(next_method=True)
            return
        
        await self.send_request(Boolean(False), String(password),
                                trivial=False)

    async def _change_password(self, prompt: str, lang: str) -> None:
        """Start password change"""

        result = await self._conn.password_change_requested(prompt, lang)

        if result == NotImplemented:
            # Password change not supported - move on to the next auth method
            self._conn.try_next_auth(next_method=True)
            return

        old_password, new_password = result

        self._password_change = True

        await self.send_request(Boolean(True),
                                String(old_password.encode('utf-8')),
                                String(new_password.encode('utf-8')),
                                trivial=False)

    def auth_succeeded(self) -> None:
        if self._password_change:
            self._password_change = False
            self._conn.password_changed()

    def auth_failed(self) -> None:
        if self._password_change:
            self._password_change = False
            self._conn.password_change_failed()

    def _process_password_change(self, _pkttype: int, _pktid: int,
                                 packet: SSHPacket) -> None:
        """Process a password change request"""

        prompt_bytes = packet.get_string()
        lang_bytes = packet.get_string()

        try:
            prompt = prompt_bytes.decode('utf-8')
            lang = lang_bytes.decode('ascii')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid password change request') from None

        self.auth_failed()
        self.create_task(self._change_password(prompt, lang))

    _packet_handlers = {
        MSG_USERAUTH_PASSWD_CHANGEREQ: _process_password_change
    }



def register_auth_method(alg: bytes, client_handler: Type[ClientAuth]) -> None:
    """Register an authentication method"""

    _auth_methods.append(alg)
    _client_auth_handlers[alg] = client_handler


def get_supported_client_auth_methods() -> Sequence[bytes]:
    """Return a list of supported client auth methods"""

    return [method for method in _client_auth_handlers
            if method != b'none']


def lookup_client_auth(conn: 'SSHClientConnection',
                       method: bytes) -> Optional[ClientAuth]:
    """Look up the client authentication method to use"""

    if method in _auth_methods:
        return _client_auth_handlers[method](conn, method)
    else:
        return None



_auth_method_list = (
    (b'none',                 _ClientNullAuth),
    (b'gssapi-keyex',         _ClientGSSKexAuth),
    (b'gssapi-with-mic',      _ClientGSSMICAuth),
    (b'hostbased',            _ClientHostBasedAuth),
    (b'publickey',            _ClientPublicKeyAuth),
    (b'keyboard-interactive', _ClientKbdIntAuth),
    (b'password',             _ClientPasswordAuth)
)

for _args in _auth_method_list:
    register_auth_method(*_args)
