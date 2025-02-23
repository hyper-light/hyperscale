import asyncio
import socket
import email.utils
import email.message
import email.generator
import re
import ssl
from collections import defaultdict
from typing import Literal, Tuple
from .protocols import SMTPConnection
from .protocols.tcp import SMTP_LIMIT

from hyperscale.core.engines.client.shared.models import (
    URL as SMTPUrl,
)
from hyperscale.core.engines.client.shared.models import (
    RequestType,
    URLMetadata,
)
from hyperscale.core.engines.client.shared.protocols import ProtocolMap
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Cookies,
    Data,
    Headers,
    Params,
)


CRLF = "\r\n"
bCRLF = b"\r\n"

class MercurySyncSMTPConnection:

    def __init__(
        self,
        pool_size: int | None = None,
        cert_path: str | None = None,
        key_path: str | None = None,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ):
        self._concurrency = pool_size
        self.timeouts = timeouts
        self.reset_connections = reset_connections

        self._cert_path = cert_path
        self._key_path = key_path
        self._ssl_context: ssl.SSLContext | None = None

        self._loop = asyncio.get_event_loop()
        self._server_name: str | None = None
        self._command_encoding: Literal['ascii', 'utf-8'] = 'ascii'


        self._dns_lock: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: list[asyncio.Future] = []

        self._client_waiters: dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: list[SMTPConnection] = []

        self._hosts: dict[str, Tuple[str, int]] = {}

        self._connections_count: dict[str, list[asyncio.Transport]] = defaultdict(list)

        self._semaphore: asyncio.Semaphore = None

        self._url_cache: dict[str, SMTPUrl] = {}

        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.TCP]
        self._optimized: dict[str, URL | Params | Headers | Auth | Data | Cookies] = {}

        self.address_family = address_family
        self.address_protocol = protocol
        self._OLDSTYLE_AUTH = re.compile(r"auth=(.*)", re.I)

    async def _ehlo(
        self,
        connection: SMTPConnection,
        server_name: str | None = None,\
    ):
        
        if server_name and self._server_name is None:
            self._server_name = server_name
        
        elif self._server_name is None:
            self._server_name = await self._get_local_server()


        ehlo_command = f"ehlo {self._server_name}{CRLF}".encode(self._command_encoding)  
        connection.write(ehlo_command)

        reply: tuple[int, bytes] = await self._get_reply(connection)
        code, msg = reply
        # According to RFC1869 some (badly written)
        # MTA's will disconnect on an ehlo. Toss an exception if
        # that happens -ddm
        if code == -1 and len(msg) == 0:
            raise Exception("Server not connected")
        
        ehlo_resp = msg

        if code != 250:
            return (code, msg, None)
        
        self.does_esmtp = True
        resp = ehlo_resp.decode("latin-1").split('\n')

        del resp[0]
        
        esmtp_features = {}
        for each in resp:
            # To be able to communicate with as many SMTP servers as possible,
            # we have to take the old-style auth advertisement into account,
            # because:
            # 1) Else our SMTP feature parser gets confused.
            # 2) There are some servers that only advertise the auth methods we
            #    support using the old style.
            if auth_match := self._OLDSTYLE_AUTH.match(each):
                # This doesn't remove duplicates, but that's no problem
                auth_feature = esmtp_features.get("auth", "")
                auth_additional = auth_match.groups(0)[0]
                esmtp_features["auth"] = f'{auth_feature} {auth_additional}'
                continue

            # RFC 1869 requires a space between ehlo keyword and parameters.
            # It's actually stricter, in that only spaces are allowed between
            # parameters, but were not going to check for that here.  Note
            # that the space isn't present if there are no parameters.
            if match := re.match(r'(?P<feature>[A-Za-z0-9][A-Za-z0-9\-]*) ?', each):
                feature = match.group("feature").lower()
                params = match.string[match.end("feature"):].strip()

                if feature == "auth":
                    auth_feature = esmtp_features.get(feature, "")
                    esmtp_features[feature] = f'{auth_feature} {params}'

                else:
                    esmtp_features[feature] = params

        return (code, msg, esmtp_features)
    
    async def _check_start_tls(
        self,
        connection: SMTPConnection,
    ):

        ehlo_command = f"STARTTLS{CRLF}".encode(self._command_encoding)  
        connection.write(ehlo_command)

        return await self._get_reply(connection)
    
    async def _get_local_server(self):

        fqdn = await self._loop.run_in_executor(
            None,
            socket.getfqdn,
        )

        if '.' in fqdn:
            return fqdn
        else:
            # We can't find an fqdn hostname, so use a domain literal
            addr = '127.0.0.1'
            try:
                hostname = await self._loop.run_in_executor(None, socket.gethostname)
                addr = await self._loop.run_in_executor(None, socket.gethostbyname, hostname)
            except socket.gaierror:
                pass
            return'[%s]' % addr

    async def _get_reply(
        self,
        connection: SMTPConnection
    ):
        reply = b''
        reply_code = -1

        while True:
            line = await connection.readline()
            line = line[:SMTP_LIMIT + 1]
            
            if not line:
                raise Exception("Connection unexpectedly closed")

            if len(line) > SMTP_LIMIT:
                raise Exception("500 Line too long.")
            
            code = line[:3]
            # Check that the error code is syntactically correct.
            # Don't attempt to read a continuation line if it is broken.
            try:
                reply_code = int(code)
            except ValueError:
                reply_code = -1
                break

            cleaned_line = line[4:].strip(b' \t\r\n')
            # Check if multiline response.
            if line[3:4] != b"-":
                reply += cleaned_line
                break

            reply += cleaned_line + b'\n' 


        return reply_code, reply
    
    async def _send_mail(
        self,
        connection: SMTPConnection,
        sender: str,
        recipients: list[str] | str,
        body: str | bytes,
        options: dict[str, str],
        mail_options: list[str] | None = None,
        recipient_options: list[str] | None = None
    ):
        
        encoded_body = body
        if isinstance(body, str):
            encoded_body = re.sub(r'(?:\r\n|\n|\r(?!\n))', CRLF, body).encode('ascii')
        
        if mail_options is None:
            mail_options = []

        if recipient_options is None:
            recipient_options = []

        esmtp_options: list[str] = []
        if 'size' in options:
            esmtp_options.append("size=%d" % len(encoded_body))

        for option in mail_options:
            esmtp_options.append(option)
        

        options_list = ''
        if len(esmtp_options) > 0:
            if 'smtputf8' in esmtp_options:
                self._command_encoding = 'utf-8'

            options_list = ' ' + ' '.join(esmtp_options)

        quoted_sender = self._quote_address(sender)
        sender_line = f'from:{quoted_sender}{options_list}'

        connection.write(
            f"mail {sender_line}{CRLF}".encode(self._command_encoding)
        )

        (code, message) =  await self._get_reply(connection)

        if code != 250:
            if code == 421:
                raise Exception(f'Err. - {code} - {message}')
          
            self._reset(connection)
            raise Exception(f'Err. - {code} - {message} - {sender}')
        

        if isinstance(recipients, str):
            recipients = [recipients]

        send_errors: dict[str, tuple[int, str]] = {}

        for recipient in recipients:
            recipient_options_string = ''
            if len(recipient_options) > 0:

                recipient_options_string = ' ' + ' '.join(recipient_options_string)

            quoted_recipient = self._quote_address(recipient)
            recipient_command = f"rcpt to:{quoted_recipient}{CRLF}".encode(self._command_encoding)
            connection.write(recipient_command)
            
            (code, message) = await self._get_reply(connection)

            if (code != 250) and (code != 251):
                send_errors[recipient] = (code, message)

            if code == 421:

                errors = ' '.join([
                    f'Err. - recipient {recipient} encountered - {code} - {message}'
                    for recipient, (code, message) in send_errors.items()
                ])

                raise Exception(errors)
            
        if len(send_errors) == len(recipients):
            # the server refused all our recipients
            await self._reset()
            raise Exception(send_errors)
        
        connection.write('data')
        (code, message) = await self._get_reply(connection)
        if code != 354:
            raise Exception(f'Err. - {code} - {message}')
        
        quoted_message = re.sub(br'(?m)^\.', b'..', encoded_body)
        if quoted_message[-2:] != bCRLF:
            quoted_message = quoted_message + bCRLF

        quoted_message = quoted_message + b"." + bCRLF

        connection.write(quoted_message)
        (code, message) = await self._get_reply(connection)
        
        if code != 250:
            if code == 421:
                raise Exception(f'Err. - {code} - {message}')
      
            await self._reset(connection)
            
            raise Exception(f'Err. - {code} - {message}')
        #if we got here then somebody got our mail
        return send_errors
    
    async def _reset(
        self,
        connection: SMTPConnection
    ):
        self.command_encoding = 'ascii'

        connection.write('rset')
        await self._get_reply(connection)

    
    def _quote_address(self, addrstring: str):
        """Quote a subset of the email addresses defined by RFC 821.

        Should be able to handle anything email.utils.parseaddr can handle.
        """
        displayname, addr = email.utils.parseaddr(addrstring)
        if (displayname, addr) == ('', ''):
            # parseaddr couldn't parse it, use it as is and hope for the best.
            if addrstring.strip().startswith('<'):
                return addrstring
            return "<%s>" % addrstring
        return "<%s>" % addr
        
 
    async def _connect_to_url_location(
        self,
        request_url: str | URL,
        connection_type: Literal['insecure', 'ssl', 'tls'] | None = None,
        is_upgrade: bool = False
    ) -> Tuple[
        Exception | None,
        SMTPConnection,
        SMTPUrl,
    ]:
        has_optimized_url = isinstance(request_url, URL)

        if has_optimized_url:
            parsed_url = request_url.optimized

        else:
            parsed_url = SMTPUrl(
                request_url,
                family=self.address_family,
                protocol=self.address_protocol,
            )

        url = self._url_cache.get(parsed_url.hostname)
        dns_lock = self._dns_lock[parsed_url.hostname]
        dns_waiter = self._dns_waiters[parsed_url.hostname]

        do_dns_lookup = url is None and has_optimized_url is False

        if do_dns_lookup and dns_lock.locked() is False:
            await dns_lock.acquire()
            url = parsed_url
            
            await url.lookup_smtp(
                url.full,
                self._loop,
                connection_type=connection_type,
            )
            
            self._dns_lock[parsed_url.hostname] = dns_lock
            self._url_cache[parsed_url.hostname] = url

            dns_waiter = self._dns_waiters[parsed_url.hostname]

            if dns_waiter.done() is False:
                dns_waiter.set_result(None)

            dns_lock.release()

        elif do_dns_lookup:
            await dns_waiter
            url = self._url_cache.get(parsed_url.hostname)

        elif has_optimized_url:
            url = request_url.optimized

        connection_error: Exception | None = None
        connection = self._connections.pop()

        if url.address is None:
            for address_info in url:
                try:
                    port = await connection.make_connection(
                        url.full,
                        address_info,
                        ssl=self._ssl_context if connection_type  in ['ssl', 'tls'] else None,
                        connection_type=connection_type,
                        timeout=self.timeouts.connect_timeout,
                        ssl_upgrade=is_upgrade,
                    )

                    url.address = address_info
                    url.port = port

                    break

                except Exception as err:
                    connection_error = err
                
        else:
            try:
                await connection.make_connection(
                    url.full,
                    url.address,
                    ssl=self._ssl_context if connection_type  in ['ssl', 'tls'] else None,
                    connection_type=connection_type,
                    timeout=self.timeouts.connect_timeout,
                    ssl_upgrade=is_upgrade,
                )

            except Exception as err:
                import traceback
                print(traceback.format_exc())
                connection_error = err

        return (
            connection_error if url.address is None else None,
            connection,
            parsed_url,
        )