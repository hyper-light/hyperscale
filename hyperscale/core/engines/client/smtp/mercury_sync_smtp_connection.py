import asyncio
import base64
import email.charset
import email.utils
import email.message
import email.generator
import hmac
import re
import ssl
import time
from base64 import encodebytes as _bencode
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
from email.base64mime import body_encode as encode_base64
from pathlib import Path
from typing import Literal, Tuple, Callable
from .protocols import SMTPConnection
from .protocols.tcp import SMTP_LIMIT

from hyperscale.core.engines.client.shared.models import URL as SMTPUrl
from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.engines.client.shared.protocols import ProtocolMap
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import (
    URL,
    Email,
)
from .models.smtp import (
    EmailAttachment,
    SMTPResponse,
    SMTPTimings,
)


CRLF = "\r\n"
bCRLF = b"\r\n"
_MAX_CHALLENGES = 5  # Maximum number of AUTH challenges sent
COMMASPACE = ', '

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

        self._loop: asyncio.AbstractEventLoop | None = None


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
        self._optimized: dict[str, URL | Email ] = {}

        self.address_family = address_family
        self.address_protocol = protocol
        self._OLDSTYLE_AUTH = re.compile(r"auth=(.*)", re.I)

        self._ehlo_command = f"ehlo [127.0.0.1]{CRLF}".encode('ascii') 
        self._check_start_tls_command = f"STARTTLS{CRLF}".encode('ascii')

        self._emails: dict[str, str] = {}

    async def send(
        self,
        server: str | URL,
        sender: str,
        recipients: str | list[str],
        subject: str,
        email: str | Email,
        auth: tuple[str, str] = None,
        attachements: EmailAttachment | list[EmailAttachment] | None = None,
    ):
        
        async with self._semaphore:
            try:

                if isinstance(email, Email):
                    self._emails[subject] = email.optimized

                email_document = self._emails.get(subject)

                if email_document is None:
                    email_document = await self.create(
                        sender,
                        recipients,
                        subject,
                        email,
                        attachments=attachements,
                    )

                    self._emails[subject] = email_document

                return await self._execute(
                    server,
                    sender,
                    recipients,
                    email_document,
                    auth=auth
                )

            except asyncio.TimeoutError:
                return SMTPResponse(
                    recipients=recipients,
                    sender=sender,
                    server=server,
                    body=email_document,
                    error=Exception("Request timed out."),
                    timings={},
                )
        
    async def create(
        self,
        sender: str,
        recipients: str | list[str],
        subject: str,
        body: str,
        attachments: EmailAttachment | list[EmailAttachment] | None = None,
    ):
        
        if isinstance(recipients, str):
            recipients = [recipients]

        email = MIMEMultipart()
        email['From'] = sender
        email['To'] = COMMASPACE.join(recipients)
        email['Date'] = formatdate(localtime=True)
        email['Subject'] = subject

        email.attach(MIMEText(body))

        if isinstance(attachments, EmailAttachment):
            attachments = [attachments]

        if attachments:
            attachment_parts = await asyncio.gather(*[
                self._attach_file(
                    attachment.path,
                    mime_type=attachment.mime_type,
                ) for attachment in attachments
            ])

            for part in attachment_parts:
                email.attach(part)

        return email.as_string()
    
    async def _optimize(
        self,
        optimized_param: URL | Email,
    ):
        if isinstance(optimized_param, URL):
            await self._optimize_url(optimized_param)

        else:
            self._optimized[optimized_param.call_name] = optimized_param

    async def _optimize_url(self, url: URL):
        try:
            upgrade_ssl: bool = False
            if url:
                (
                    _,
                    connection,
                    url,
                    upgrade_ssl,
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(url),
                    timeout=self.timeouts.connect_timeout,
                )

                self._connections.append(connection)

            if upgrade_ssl:
                url.data = url.data.replace("http://", "https://")

                await url.optimize()

                (
                    _,
                    connection,
                    url,
                    _,
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(url),
                    timeout=self.timeouts.connect_timeout,
                )

                self._connections.append(connection)

            self._url_cache[url.optimized.hostname] = url
            self._optimized[url.call_name] = url

        except Exception:
            pass
     
    async def _attach_file(
        self,
        filepath: str,
        mime_type: str = 'application/octet-stream'
    ):
        mime_base, mime_subtype = mime_type.split('/', maxsplit=1)
        part = MIMEBase(mime_base, mime_subtype)

        attachment_file = await self._loop.run_in_executor(
            None,
            open,
            filepath,
            'rb'
        )

        part.set_payload(
            await self._loop.run_in_executor(
                None,
                attachment_file.read
            )
        )

        encoders.encode_base64(part)

        orig = part.get_payload(decode=True)
        encdata = str(_bencode(orig), 'ascii')
        part.set_payload(encdata)
        part['Content-Transfer-Encoding'] = 'base64'

        path = await self._loop.run_in_executor(
            None,
            Path,
            filepath
        )

        part.add_header(
            'Content-Disposition',
            f'attachment; filename={path.name}'
        )

        return part

    async def _execute(
        self,
        server: str,
        sender: str,
        recipients: list[str] | str,
        email: str,
        auth: tuple[str, str] = None,
    ):
        timings: SMTPTimings = {
            "request_start": None,
            "connect_start": None,
            "connect_end": None,
            "ehlo_start": None,
            "ehlo_end": None,
            "tls_check_start": None,
            "tls_check_end": None,
            "tls_upgrade_start": None,
            "tls_upgrade_end": None,
            "ehlo_tls_start": None,
            "ehlo_tls_end": None,
            "login_start": None,
            "login_end": None,
            "send_mail_start": None,
            "send_mail_end": None,
            "request_end": None,
        }


        timings['request_start'] = time.monotonic()
        
        try:
            timings["connect_start"] = time.monotonic()

            (
                err,
                connection,
                url
            ) = await self._connect_to_url_location(server)

            timings["connect_end"] = time.monotonic()

            if err:
                timings["request_end"] = time.monotonic()
                return SMTPResponse(
                    recipients=recipients,
                    sender=sender,
                    email=email,
                    server=server,
                    error=err,
                    timings=timings,
                    encoding=connection.command_encoding,
                )
            
            timings["server_ack_start"] = time.monotonic()
            
            (code, message, err) = await self._get_reply(connection)

            timings["server_ack_send"] = time.monotonic()

            if code != 220:
                err = Exception(f'Err. - {code} - {message}')

            if err:
                timings["request_end"] = time.monotonic()
                return SMTPResponse(
                    recipients=recipients,
                    sender=sender,
                    email=email,
                    server=server,
                    error=err,
                    last_smtp_code=code,
                    last_smtp_message=message,
                    timings=timings,
                    encoding=connection.command_encoding,
                )
                
            timings["ehlo_start"] = time.monotonic()

            (
                code,
                message,
                options,
                err,
            ) = await self._ehlo(connection)

            timings["ehlo_end"] = time.monotonic()

            if options is None or code != 250:
                err = Exception(f'Err. - {code} - {message}')

            if err:
                timings["request_end"] = time.monotonic()
                return SMTPResponse(
                    recipients=recipients,
                    sender=sender,
                    email=email,
                    server=server,
                    error=err,
                    last_smtp_code=code,
                    last_smtp_message=message,
                    last_smtp_options=options,
                    timings=timings,
                    encoding=connection.command_encoding,
                )
            
            resend_ehlo = False

            if 'starttls' in options and url.port == 587:

                timings["tls_check_start"] = time.monotonic()


                connection.write(self._check_start_tls_command)
                (code, message, err) = await self._get_reply(connection)

                timings["tls_check_end"] = time.monotonic()


                if code == 220 and err is None:

                    timings["tls_upgrade_start"] = time.monotonic()

                    await connection.make_connection(
                        server,
                        url.address,
                        ssl=self._ssl_context,
                        connection_type='tls',
                        ssl_upgrade=True,
                        timeout=self.timeouts.connect_timeout,
                    )

                    timings["tls_upgrade_end"] = time.monotonic()

                    resend_ehlo = True

                else:
                    err = Exception(f'Err. - {code} {message}')
            
            elif  'starttls' in options and url.port == 465:

                timings["tls_upgrade_start"] = time.monotonic()

                await connection.make_connection(
                    server,
                    url.address,
                    ssl=self._ssl_context,
                    connection_type='ssl',
                    ssl_upgrade=True,
                    timeout=self.timeouts.connect_timeout,
                )
                
                timings["tls_upgrade_end"] = time.monotonic()

                resend_ehlo = True

            if err:
                return SMTPResponse(
                    recipients=recipients,
                    sender=sender,
                    email=email,
                    server=server,
                    error=err,
                    last_smtp_code=code,
                    last_smtp_message=message,
                    last_smtp_options=options,
                    timings=timings,
                    encoding=connection.command_encoding,
                )

            if resend_ehlo:

                timings["ehlo_tls_start"] = time.monotonic()

                (
                    code,
                    message,
                    options,
                    err,
                ) = await self._ehlo(connection)

                timings["ehlo_tls_end"] = time.monotonic()

                if options is None or code != 250:
                    err = Exception(f'Err. - {code} - {message}')

            if err:
                timings["request_end"] = time.monotonic()
                return SMTPResponse(
                    recipients=recipients,
                    sender=sender,
                    email=email,
                    server=server,
                    error=err,
                    last_smtp_code=code,
                    last_smtp_message=message,
                    last_smtp_options=options,
                    timings=timings,
                    encoding=connection.command_encoding,
                )
                
            if 'auth' in options and auth and len(auth) == 2:

                username, password = auth

                timings["login_start"] = time.monotonic()

                (
                    code,
                    message,
                    err
                ) = await self._login(
                    connection,
                    username,
                    password,
                    options,
                )
                
                timings["login_end"] = time.monotonic()

            if err:
                timings["request_end"] = time.monotonic()
                return SMTPResponse(
                    recipients=recipients,
                    sender=sender,
                    email=email,
                    server=server,
                    error=err,
                    last_smtp_code=code,
                    last_smtp_message=message,
                    last_smtp_options=options,
                    timings=timings,
                    encoding=connection.command_encoding,
                )
            
            timings["send_mail_start"] = time.monotonic()
            
            if isinstance(recipients, str):
                recipients = [recipients]
            
            (
                code,
                message,
                err,
            ) = await self._send_mail(
                connection,
                sender,
                recipients,
                email,
                options,
            )

            timings["send_mail_end"] = time.monotonic()
            timings["request_end"] = time.monotonic()

            self._connections.append(connection)

            return SMTPResponse(
                recipients=recipients,
                sender=sender,
                email=email,
                server=server,
                error=err,
                last_smtp_code=code,
                last_smtp_message=message,
                last_smtp_options=options,
                timings=timings,
                encoding=connection.command_encoding,
            )
        
        except Exception as err:
            self._connections.append(
                SMTPConnection(
                    reset_connections=self.reset_connections,
                )
            )

            return SMTPResponse(
                recipients=recipients,
                sender=sender,
                server=server,
                email=email,
                error=err,
                timings=timings,
                encoding=connection.command_encoding,
            )

    async def _ehlo(
        self,
        connection: SMTPConnection,
    ):

        connection.write(self._ehlo_command)

        reply: tuple[int, bytes] = await self._get_reply(connection)
        code, message, err = reply

        if err:
            return (
                code,
                message,
                None,
                err
            )
        
        elif code == -1 and len(message) == 0:
            return (
                code,
                message,
                None,
                Exception("Server not connected")
            )

        if code != 250:
            return (code, message, None, None)
        
        self.does_esmtp = True
        resp = message.decode("latin-1").split('\n')

        del resp[0]
        
        esmtp_features = {}
        for each in resp:
            if auth_match := self._OLDSTYLE_AUTH.match(each):
                # This doesn't remove duplicates, but that's no problem
                auth_feature = esmtp_features.get("auth", "")
                auth_additional = auth_match.groups(0)[0]
                esmtp_features["auth"] = f'{auth_feature} {auth_additional}'
                continue

            if match := re.match(r'(?P<feature>[A-Za-z0-9][A-Za-z0-9\-]*) ?', each):
                feature = match.group("feature").lower()
                params = match.string[match.end("feature"):].strip()

                if feature == "auth":
                    auth_feature = esmtp_features.get(feature, "")
                    esmtp_features[feature] = f'{auth_feature} {params}'

                else:
                    esmtp_features[feature] = params

        return (
            code, 
            message, 
            esmtp_features, 
            None,
        )
    

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
                return (
                    None,
                    None,
                    Exception("Connection unexpectedly closed"),
                )

            if len(line) > SMTP_LIMIT:
                return (
                    None,
                    None,
                    Exception("500 Line too long."),
                )
            
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


        return (
            reply_code, 
            reply,
            None,
        )
    
    async def _login(
        self, 
        connection: SMTPConnection,
        user: str, 
        password: str,
        options: dict[str, str],
        initial_response_ok=True,
    ):

        advertised_authlist = options["auth"].split()
        preferred_auths = ['CRAM-MD5', 'PLAIN', 'LOGIN']

        authlist = [
            auth for auth in preferred_auths if auth in advertised_authlist
        ]

        if not authlist:
            return (
                None,
                None,
                Exception("No suitable authentication method found.")
            )

        authorization_call: Callable[
            ...,
            str | None
        ] | None = None
        last_exception: Exception | None = None

        
        for auth_method in authlist:

            match auth_method:
                case 'CRAM-MD5':
                    authorization_call = self._auth_cram_md5

                case 'PLAIN':
                    authorization_call = self._auth_plain

                case 'LOGIN':
                    authorization_call = self._auth_login

                case _:
                    pass

            if authorization_call:

        
                try:
                    (code, message, err) = await self._auth(
                        connection,
                        auth_method, 
                        authorization_call,
                        (user, password),
                        initial_response_ok=initial_response_ok,
                    )

                    if err or code not in [235, 503]:
                        return (
                            code,
                            message,
                            err
                        )

                    return (
                        code, 
                        message,
                        None,
                    )
                
                except Exception as e:
                    last_exception = e

        return (
            None,
            None,
            last_exception
        )

    async def _auth(
        self, 
        connection: SMTPConnection,
        auth_method: str, 
        authorization_call: Callable[
            [
                str,
                str,
                int,
                str | None
            ],
            str | None
        ], 
        login_info: tuple[str, str],
        initial_response_ok=True,
    ):
        
        username, password = login_info

        auth_challenge_count = 0

        initial_response: str | None
        if initial_response_ok:
            initial_response = authorization_call(
                username, 
                password,
                auth_challenge_count,
            )
        
        if initial_response is not None:
            response = encode_base64(initial_response.encode('ascii'), eol='')

            connection.write(f'AUTH {auth_method} {response}{CRLF}'.encode(connection.command_encoding))
            (code, message, err) = await self._get_reply(connection)

            auth_challenge_count = 1

        else:
            connection.write(f"AUTH {auth_method}{CRLF}".encode(connection.command_encoding))

            (code, message, err) = await self._get_reply(connection)
            auth_challenge_count = 0

        if err:
            return (
                code,
                message,
                err,
            )
            
        while code == 334:
            auth_challenge_count += 1
            challenge = base64.decodebytes(message)
            response = encode_base64(
                authorization_call(
                    username,
                    password,
                    auth_challenge_count,
                    challenge
                ).encode('ascii'), 
                eol='',
            )

            response_command = f'{response}{CRLF}'
            connection.write(response_command.encode(connection.command_encoding))
            (code, message) = await self._get_reply(connection)

            # If server keeps sending challenges, something is wrong.
            if auth_challenge_count > _MAX_CHALLENGES:
                return (
                    None,
                    None,
                    Exception(
                        f"Server AUTH mechanism infinite loop. Last response - {code} - {message}"
                    )
                )
            
        if code not in (235, 503):
            return (
                None,
                None,
                Exception(f'Err. - {code} - {message}')  
            ) 
        
        return (
            code, 
            message,
            None,
        )
    
    def _auth_cram_md5(
        self, 
        user: str,
        password:str,
        _: int,
        challenge: str | None = None,
    ):
        """ Authobject to use with CRAM-MD5 authentication. Requires self.user
        and self.password to be set."""
        # CRAM-MD5 does not support initial-response.
        if challenge is None:
            return None
        return user + " " + hmac.HMAC(
            password.encode('ascii'), challenge, 'md5').hexdigest()

    def _auth_plain(
        self,
        user: str,
        password:str,
        _: int,
    ):
        """ Authobject to use with PLAIN authentication. Requires self.user and
        self.password to be set."""
        return "\0%s\0%s" % (user, password)

    def _auth_login(
        self,
        user: str,
        password:str,
        auth_challenge_count: int,
        challenge: str | None = None,
    ):
        """ Authobject to use with LOGIN authentication. Requires self.user and
        self.password to be set."""
        if challenge is None or auth_challenge_count < 2:
            return user
        else:
            return password

    
    async def _send_mail(
        self,
        connection: SMTPConnection,
        sender: str,
        recipients: list[str],
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
                connection.command_encoding = 'utf-8'

            options_list = ' ' + ' '.join(esmtp_options)

        quoted_sender = self._quote_address(sender)
        sender_line = f'from:{quoted_sender}{options_list}'

        connection.write(
            f"mail {sender_line}{CRLF}".encode(connection.command_encoding)
        )

        (code, message, err) =  await self._get_reply(connection)

        if err:
            return (
                code,
                message,
                err
            )

        elif code == 421:
            return (
                code,
                message,
                Exception(f'Err. - {code} - {message}'),
            )

        elif code != 250:
            await self._reset(connection)
            return (
                code,
                message,
                Exception(f'Err. - {code} - {message} - {sender}')
            )
        

        recipient_options_string = ''
        if len(recipient_options) > 0:
            recipient_options_string = ' ' + ' '.join(recipient_options_string) 

        results: dict[
            str,
            tuple[int, str, Exception]
        ] = {}

        send_errors = 0

        for recipient in recipients:

            quoted_recipient = self._quote_address(recipient)
            recipient_command = f"rcpt to:{quoted_recipient}{CRLF}".encode(connection.command_encoding)
            connection.write(recipient_command)
            
            (code, message, err) = await self._get_reply(connection)

            results[recipient] = (
                code,
                message,
                err,
            )

            if err:
                send_errors += 1
    
            elif (code != 250) and (code != 251):
                send_errors += 1

            elif code == 421:
                return (
                    code,
                    message,
                    Exception(f'Err. - {code} - {message} - for recipient - {recipient}')
                )
            
        if send_errors == len(recipients):
            await self._reset()

            return (
                None,
                None,
                Exception('Delivery failed for all recipients'),
            )
            
        connection.write(f'data{CRLF}'.encode(connection.command_encoding))
        (code, message, err) = await self._get_reply(connection)

        if err:
            return (
                code,
                message,
                err,
            )
        
        elif code != 354:
            return (
                code,
                message,
                Exception(f'Err. - {code} - {message}'),
            )
        
        quoted_message = re.sub(br'(?m)^\.', b'..', encoded_body)
        if quoted_message[-2:] != bCRLF:
            quoted_message = quoted_message + bCRLF

        quoted_message = quoted_message + b"." + bCRLF

        connection.write(quoted_message)
        (code, message, err) = await self._get_reply(connection)

        if err:
            return (
                code,
                message,
                err,
            )
        
        elif code == 421:
            return (
                code,
                message,
                Exception(f'Err. - {code} - {message}'),
            )
        
        elif code != 250:
            await self._reset(connection)        
            return (
                code,
                message,
                Exception(f'Err. - {code} - {message}'),
            )
        
        return (
            code,
            message,
            None
        )
    
    async def _reset(
        self,
        connection: SMTPConnection
    ):
        connection.command_encoding = 'ascii'

        connection.write(f'rset{CRLF}'.encode(connection.command_encoding))
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
    
    async def _noop(
        self,
        connection: SMTPConnection
    ):
        connection.write(f'noop{CRLF}'.encode(connection.command_encoding))
        return await self._get_reply(connection)
    
    async def _verify(
        self,
        connection: SMTPConnection,
        email_address: str
    ):
        cleaned_address = email_address
        display_name, addr = email.utils.parseaddr(email_address)
        if (display_name, addr) != ('', ''):
            # parseaddr couldn't parse it, so use it as is.
            cleaned_address = addr

        verify_command = f'vrfy {cleaned_address}{CRLF}'.encode(connection.command_encoding)

        connection.write(verify_command)
        (code, message) =  await self._get_reply()

        if code not in [250, 251]:
            return (
                code,
                message,
                Exception(f'Err. - {code} - {message}')
            )
        
        return (
            code,
            message,
            None,
        )
 
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

                    parsed_url.address = address_info
                    parsed_url.port = port

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
                connection_error = err

        
        return (
            connection_error if parsed_url.address is None else None,
            connection,
            parsed_url,
        )