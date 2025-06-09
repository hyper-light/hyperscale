import asyncio
import ssl
import re
import socket
from collections import defaultdict
from typing import Tuple, Literal, Any

from hyperscale.core.engines.client.shared.models import URL as FTPUrl
from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.engines.client.shared.protocols import ProtocolMap
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Data
)
from hyperscale.core.engines.client.ftp.models.ftp import ConnectionType, CRLF
from hyperscale.core.engines.client.ftp.protocols import FTPConnection
from hyperscale.core.engines.client.ftp.protocols.tcp import MAXLINE




class MercurySyncFTPConnection:

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


        self._dns_lock: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: list[asyncio.Future] = []

        self._client_waiters: dict[asyncio.Transport, asyncio.Future] = {}
        self._control_connections: list[FTPConnection] = []
        self._data_connections: list[FTPConnection] = []


        self._hosts: dict[str, Tuple[str, int]] = {}

        self._connections_count: dict[str, list[asyncio.Transport]] = defaultdict(list)

        self._semaphore: asyncio.Semaphore = None

        self._url_cache: dict[str, FTPUrl] = {}

        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.FTP]
        self._optimized: dict[str, URL | Auth | Data ] = {}

        self.address_family = address_family
        self.address_protocol = protocol
        self._is_secured: bool = False
        
        self._227_re = re.compile(
            r'(\d+),(\d+),(\d+),(\d+),(\d+),(\d+)', 
            re.ASCII,
        )

        self._150_re = re.compile(
            r"150 .* \((\d+) bytes\)", 
            re.IGNORECASE | re.ASCII,
        )

    async def _execute(
        self,
        url: str | URL,
        action: Literal[
            'CHANGE_DIRECTORY',
            'LIST', 
            'MAKE_DIRECTORY',
            'PWD',
            'RECEIVE', 
            'REMOVE_FILE',
            'REMOVE_DIRECTORY',
            'RENAME_FILE', 
            'SEND', 
        ],
        data: str | Data | None = None,
        auth: tuple[str, str, str] = None,
        options: list[str] = [],
        secure_connection: bool = True,
    ):
        
        control_connection: FTPConnection | None = None
        
        try:
            (
                err,
                control_connection,
                url
            ) = await self._connect_to_url_location(url)

        except Exception as err:
            self._control_connections.append(
                FTPConnection(
                    reset_connections=self.reset_connections,
                )
            )

            return (
                None,
                err
            )
        
        if err:
            return (
                None,
                err,
            )

        data_connection: FTPConnection | None = None
        
        try:
            (
                _,
                err,
            ) = await self._get_response(control_connection)

            if err:
                return (
                    None,
                    err
                )
            
            if control_connection.logged_in is False:
                await control_connection.login_lock.acquire()
                (
                    control_connection,
                    err
                ) = await self._login(
                    control_connection,
                    auth=auth,
                )
                
                control_connection.login_lock.release()

            if err:
                return (
                    None,
                    err,
                )
            
            if secure_connection and control_connection.secure is False:
                await control_connection.secure_lock.acquire()

                (
                    control_connection,
                    err
                ) = await self._secure_connection(control_connection)
            
                control_connection.secure_lock.release()
            
            if err:
                return (
                    None,
                    err,
                )

            result: Any | None = None

            match action:

                case 'CHANGE_DIRECTORY':
                    (
                        result,
                        err,
                    ) = await self._change_directory(
                        control_connection,
                        data,
                    )

                case 'LIST':
                    (
                        data_connection,
                        result,
                        err
                    ) = await self._list(
                        control_connection,
                        url,
                        data,
                        options=options,
                    )

                case 'MAKE_DIRECTORY':
                    (
                        result,
                        err,
                    ) = await self._mkdir(control_connection)

                case 'PWD':
                    (
                        control_connection,
                        result,
                        err,
                    ) = await self._pwd(control_connection)

                case 'RECEIVE':
                    pass

                case 'REMOVE_FILE':
                    pass

                case 'REMOVE_DIRECTORY':
                    pass

                case 'RENAME_FILE':
                    pass

                case 'SEND':
                    pass


                case _:
                    return (
                        None,
                        Exception('Unsupported action')
                    )
                
            if data_connection:
                self._data_connections.append(
                    FTPConnection(reset_connections=self.reset_connections)
                )

            if err:
                return (
                    None,
                    err,
                )
            
            return (
                result,
                None,
            )

        except Exception as err:
            if data_connection:
                data_connection.close()
                self._data_connections.append(
                    FTPConnection(reset_connections=self.reset_connections)
                )

            self._control_connections.append(
                FTPConnection(
                    reset_connections=self.reset_connections,
                )
            )

            return (
                None,
                err
            )
        
    async def _login(
        self, 
        connection: FTPConnection,
        auth: tuple[str, str, str] | None = None
    ):
        
        username = b'anonymous'
        password = b''
        account = b''

        if auth:
            (
                username,
                password,
                account
            ) = auth

            username = username.encode()
            password = password.encode()
            account = account.encode()

        if username == b'anonymous' and password in [b'', b'-']:
            # If there is no anonymous ftp password specified
            # then we'll just use anonymous@
            # We don't send any other thing because:
            # - We want to remain anonymous
            # - We want to stop SPAM
            # - We don't want to let ftp sites to discriminate by the user,
            #   host or country.
            password = password + b'anonymous@'


        username_command = b'USER ' + username
        connection.write(username_command + CRLF)
        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            return (
                None,
                err,
            )
        

        if response[0] == 51:
            password_command = b'PASS ' + password
            connection.write(password_command + CRLF)

            (
                response,
                err
            ) = await self._get_response(connection)
        
        if err:
            return (
                None,
                err,
            )
        
        if response[0] == 51:
            account_command = b'ACCT ' + account
            connection.write(account_command + CRLF)


            (
                response,
                err
            ) = await self._get_response(connection)

        if err:
            return (
                None,
                err,
            )

        if response[0] != 50:
            return (
                None,
                Exception(response.decode())
            )
        
        return (
            connection,
            err,
        )
    
    async def _secure_connection(
        self,
        connection: FTPConnection,
    ):
        pbsz_command = b'PBSZ 0'
        connection.write(pbsz_command + CRLF)
        (
            _,
            err
        ) = await self._get_response(connection)

        if err:
            return (
                None,
                err,
            )
        
        prot_p_command = b'PROT P'
        connection.write(prot_p_command + CRLF)
        (
            _,
            err
        ) = await self._get_response(connection)

        if err:
            return (
                None,
                err,
            )
        
        self._is_secured = True

        return (
            connection,
            None,
        )

    
    async def _list(
        self,
        connection: FTPConnection,
        url: FTPUrl,
        path: str | None = None,
        options: list[str] = []
    ):
        err: Exception | None = None

        if options:
            list_options = ";".join(options)
            list_command = f"OPTS MLST {list_options};".encode()
            
            connection.write(list_command + CRLF)

            (
                _,
                err
            ) = await self._get_response(connection)

        if err:
            return (
                None,
                None,
                err,
            )


        if path:
            command = f"MLSD {path}".encode()
        else:
            command = "MLSD".encode()

        (
            data_connection,
            lines,
            err,
        ) = await self._return(
            'TYPE A',
            connection,
            url,
            command

        )

        if err:
            return (
                data_connection,
                None,
                err,
            )

        entries: list[dict[bytes, bytes]] = []
        for line in lines:
            facts_found, _, name = line.rstrip(CRLF).partition(b' ')
            entry: dict[bytes, bytes] = {}

            for fact in facts_found[:-1].split(b";"):
                key, _, value = fact.partition(b"=")
                entry[key.lower()] = value

                entries.append(entry)

        return (
            data_connection,
            entries,
            None,
        )
    
    async def _mkdir(
        self,
        connection: FTPConnection,
        path: str,
    ):
        mkdir_command = f'MKD {path}'.encode()

        connection.write(mkdir_command + CRLF)
        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            return (
                connection,
                None,
                err,
            )
        
        if not response[:3] != b'257':
            return (
                connection,
                None,
                Exception('Unknown error occured during MKD command')
            )
        

        elif response[3:5] != b' "':
            return (
                connection,
                b'',
                None, # Not compliant to RFC 959, but UNIX ftpd does this
            )
        
        dirname = b''

        idx = 5
        response_length = len(response)

        while idx < response_length:
            current_char = response[idx]
            idx = idx+1

            if current_char == b'"':
                if idx >= response_length or response[idx] != b'"':
                    break

                idx = idx+1

            dirname = dirname + current_char

        return (
            connection,
            dirname,
            None,
        )
    
    async def _rename(
        self,
        connection: FTPConnection,
        from_name: str,
        to_name: str,
    ):
        rnfr_command = f'RNFR {from_name}'.encode()
        connection.write(rnfr_command + CRLF)

        (
            _,
            err
        ) = await self._get_response(connection)

        if err:
            return (
                connection,
                None,
                err,
            )
        
        rnto_command = f'RNTO {to_name}'.encode()
        connection.write(rnto_command + CRLF)
        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            return (
                connection,
                None,
                err,
            )
        
        if response[:1] != b'2':
            return (
                connection,
                None,
                Exception(response.decode())
            )
        
        return (
            connection,
            response,
            None,
        )
    
    async def _pwd(
        self,
        connection: FTPConnection
    ):
        pwd_command = b'PWD'

        connection.write(pwd_command + CRLF)
        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            return (
                connection,
                None,
                err,
            )
        
        if not response[:3] != b'257':
            return (
                connection,
                None,
                Exception('Unknown error occured during PWD command')
            )
        

        elif response[3:5] != b' "':
            return (
                connection,
                b'',
                None, # Not compliant to RFC 959, but UNIX ftpd does this
            )
        
        dirname = b''

        idx = 5
        response_length = len(response)

        while idx < response_length:
            current_char = response[idx]
            idx = idx+1

            if current_char == b'"':
                if idx >= response_length or response[idx] != b'"':
                    break

                idx = idx+1

            dirname = dirname + current_char

        return (
            connection,
            dirname,
            None,
        )
        
    async def _remove_directory(
        self,
        connection: FTPConnection,
        path: str,
    ):
        mkdir_command = f'RMD {path}'.encode()

        connection.write(mkdir_command + CRLF)
        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            return (
                connection,
                None,
                err,
            )
        
        
        if response[:1] != b'2':
            return (
                connection,
                None,
                Exception(response.decode())
            )
        
        return (
            connection,
            response,
            None,
        )
    
    async def _remove_file(
        self,
        connection: FTPConnection,
        path: str,
    ):
        mkdir_command = f'DELE {path}'.encode()

        connection.write(mkdir_command + CRLF)
        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            return (
                connection,
                None,
                err,
            )
        

        if response[:3] in {b'250', b'200'}:
            return (
                connection,
                response,
                None,
            )
        
        return (
            connection,
            None,
            Exception(response.decode()),
        )

    async def _return(
        self, 
        return_type: Literal['TYPE A', 'TYPE I'],
        connection: FTPConnection,
        url: FTPUrl,
        command: str,
        block_size: int = 8192
    ):
        """Retrieve data in line mode.  A new port is created for you.

        Args:
          cmd: A RETR, LIST, or NLST command.
          callback: An ohostptional single parameter callable that is called
                    for each line with the trailing CRLF stripped.
                    [default: print_line()]

        Returns:
          The response code.
        """

        connection.write(return_type.encode() + CRLF)

        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            return (
                None,
                None,
                err,
            )

        (
            data_connection,
            _,
            err
        ) = await self._initiate_transfer(
            connection,
            url,
            command,
        )

        if err:
            return (
                data_connection,
                None,
                err,
            )
        lines: list[bytes] = []
        raw_bytes = bytearray()


        try:
            if return_type == 'TYPE A':

                while True:

                    line = await data_connection.readline(
                        num_bytes=MAXLINE + 1, 
                        exit_on_eof=True,
                    )

                    if len(line) > MAXLINE:
                        return (
                            data_connection,
                            None,
                            Exception("got more than %d bytes" % MAXLINE),
                        )
                    
                    if not line:
                        break

                    if line[-2:] == CRLF:
                        line = line[:-2]
                        
                    elif line[-1:] == b'\n':
                        line = line[:-1]

                    lines.append(line)

            elif return_type == 'TYPE I':
                while data := await asyncio.wait_for(
                    data_connection.read(num_bytes=block_size),
                    timeout=self.timeouts.read_timeout,
                ):
                    raw_bytes.extend(data)

            data_connection.close()

            (
                response,
                err
            ) =  await self._get_response(connection)


            if err:
                return (
                    data_connection,
                    None,
                    err
                )
            
            if response[0] != 50:
                return (
                    data_connection,
                    None,
                    Exception(response)
                )
            
            if return_type == 'TYPE A':
                return (
                    data_connection,
                    lines,
                    None,
                )
            
            return (
                data_connection,
                raw_bytes,
                None,
            )
        
        except Exception as err:
            return (
                data_connection,
                None,
                err,
            )
    
    async def _initiate_transfer(
        self, 
        connection: FTPConnection,
        url: FTPUrl,
        command: bytes, 
        rest: str = None,
        trust_foreign_host: bool = False,
    ):
        """Initiate a transfer over the data connection.

        If the transfer is active, send a port command and the
        transfer command, and accept the connection.  If the server is
        passive, send a pasv command, connect to it, and start the
        transfer command.  Either way, return the socket for the
        connection and the expected size of the transfer.  The
        expected size may be None if it could not be determined.

        Optional `rest' argument can be a string that is sent as the
        argument to a REST command.  This is essentially a server
        marker used to tell the server to skip over any data up to the
        given marker.
        """
        if connection.socket_family == socket.AF_INET:
            connection.write('PASV'.encode() + CRLF)
            (
                response,
                err
            ) = await self._get_response(connection)

            if err:
                return (
                    None,
                    None,
                    err,
                )
            
            decoded_response = response.decode()

            if decoded_response[:3] != '227':
                return (
                    None,
                    None,
                    Exception(response)
                )
    
            matches = self._227_re.search(decoded_response)

            if not matches:
                return (
                    None,
                    None,
                    Exception(decoded_response)
                )
            
            numbers = matches.groups()
            untrusted_host = '.'.join(numbers[:4])
            
            port = (int(numbers[4]) << 8) + int(numbers[5])

            if trust_foreign_host:
                host = untrusted_host
            else:
                host = connection.host

            
        else:
            # host, port = parse229(self.sendcmd('EPSV'), connection.host)
            
            connection.write('EPSV'.encode() + CRLF)

            (
                response,
                err
            ) = await self._get_response(connection)

            if err:
                return (
                    None,
                    None,
                    err,
                )
            
            decoded_response = response.decode()

            if decoded_response[:3] != '229':
                return (
                    None,
                    None,
                    Exception(decoded_response)
                )
            
            left = decoded_response.find('(')

            if left < 0:
                return (
                    None,
                    None,
                    Exception(decoded_response)
                )

            right = decoded_response.find(')', left + 1)
            if right < 0:
                return (
                    None,
                    None,
                    Exception(decoded_response)
                )
            
            if decoded_response[left + 1] != decoded_response[right - 1]:
                return (
                    None,
                    None,
                    Exception(decoded_response)
                )
            
            parts = decoded_response[left + 1:right].split(decoded_response[left+1])
            if len(parts) != 5:
                return (
                    None,
                    None,
                    Exception(decoded_response)
                )
            
            host = connection.host
            port = int(parts[3])

        
        (
            err,
            data_connection,
            _
        ) = await self._connect_to_url_location(
            host,
            connection_type='data',
            port=port,
            control_url=url,
        )

        if err:
            return (
                data_connection,
                None,
                err
            )

        try:
            if rest is not None:
                rest_command = f'REST {rest}'.encode()
                connection.write(rest_command + CRLF)
                
                (
                    response,
                    err
                ) = await self._get_response(connection)

            if err:
                return (
                    data_connection,
                    None,
                    err,
                )
            
            connection.write(command + CRLF)

            (
                response,
                err
            ) = await self._get_response(connection)


            if err:
                return (
                    data_connection,
                    None,
                    err,
                )
            
            # Some servers apparently send a 200 reply to
            # a LIST or STOR command, before the 150 reply
            # (and way before the 226 reply). This seems to
            # be in violation of the protocol (which only allows
            # 1xx or error messages for LIST), so we just discard
            # this response.
            decoded_response = response.decode()
            if decoded_response[0] == '2':
                (
                    response,
                    err,
                ) = await self._get_response(connection)

            if err:
                return (
                    data_connection,
                    None,
                    err,
                )
            
            decoded_response = response.decode()
            if decoded_response[0] != '1':
                return (
                    data_connection,
                    None,
                    Exception(response),
                )
            
        except Exception as err:
            return (
                data_connection,
                None,
                err,
            )

        size: int = None
        

        if decoded_response[:3] == '150':
     
            matches = self._150_re.match(decoded_response)

            if matches:
                size = int(matches.group(1))

        return (
            data_connection,
            size,
            None,
        )
    
    async def get_passive_port(
        self,
        connection: FTPConnection,
        trust_foreign_host: bool = False
    ):
        """Internal: Does the PASV or EPSV handshake -> (address, port)"""
        if connection.socket_family == socket.AF_INET:
            connection.write('PASV'.encode() + CRLF)
            (
                response,
                err
            ) = await self._get_response(connection)

            if err:
                return (
                    None,
                    err,
                )
            
            decoded_response = response.decode()

            if decoded_response[:3] != '227':
                return (
                    None,
                    Exception(response)
                )
    
            matches = self._227_re.search(decoded_response)

            if not matches:
                return (
                    None,
                    Exception(decoded_response)
                )
            
            numbers = matches.groups()
            untrusted_host = '.'.join(numbers[:4])
            
            port = (int(numbers[4]) << 8) + int(numbers[5])

            if trust_foreign_host:
                host = untrusted_host
            else:
                host = connection.host
        else:
            # host, port = parse229(self.sendcmd('EPSV'), connection.host)
            
            connection.write('EPSV'.encode() + CRLF)

            (
                response,
                err
            ) = await self._get_response(connection)

            if err:
                return (
                    None,
                    err,
                )
            
            decoded_response = response.decode()

            if decoded_response[:3] != '229':
                return (
                    None,
                    Exception(decoded_response)
                )
            
            left = decoded_response.find('(')

            if left < 0:
                return (
                    None,
                    Exception(decoded_response)
                )

            right = decoded_response.find(')', left + 1)
            if right < 0:
                return (
                    None,
                    Exception(decoded_response)
                )
            
            if decoded_response[left + 1] != decoded_response[right - 1]:
                return (
                    None,
                    Exception(decoded_response)
                )
            
            parts = decoded_response[left + 1:right].split(decoded_response[left+1])
            if len(parts) != 5:
                return (
                    None,
                    Exception(decoded_response)
                )
            
            host = connection.host
            port = int(parts[3])

        return host, port
        
    async def _change_directory(
        self,
        connection: FTPConnection,
        directory: str,
    ):
        if '\r' in directory or '\n' in directory:
            return (
                None,
                ValueError('an illegal newline character should not be contained'),
            )
        
        if dirname == '..':
            try:

                connection.write(directory.encode() + CRLF)

                (
                    response,
                    err,
                ) = await self._get_response(connection)
                if response[:1] != '2':
                    return (
                        None,
                        Exception(response.decode())
                    )
                    
            except Exception as err:
                if err.args[0][:3] != '500':
                    return (
                        None,
                        err
                    )

        elif dirname == '':
            dirname = '.'  # does nothing, but could return error

        cmd = 'CWD ' + dirname

        connection.write(cmd + CRLF)

        return await self._get_response(connection)
    
    async def _get_response(
        self,
        connection: FTPConnection
    ):
        """Expect a response beginning with '2'."""
        (
            line,
            err
        ) = await self._get_line(connection)

        if line is None or err:
            return (
                None,
                err
            )

        if line[3:4] == b'-':
            code = line[:3]
            while True:
                (
                    next_line,
                    err,
                ) = await self._get_line(connection)

                if next_line is None or err:
                    return (
                        None,
                        err
                    )

                line = line + (b'\n' + next_line)
                if next_line[:3] == code and \
                        next_line[3:4] != b'-':
                    break

        response_code = line[:1]
        response: bytes | None = None

        if response_code in {b'1', b'2', b'3'}:
            response = line

        elif response_code == b'4':
            err = Exception(line.decode())

        elif response_code == b'5':
            err = Exception(line.decode())
        
        if response is None:
            err = Exception(line.decode())

        if err:
            return (
                None,
                err,
            )

        return (
            response,
            None
        )
    
    async def _get_line(
        self,
        connecton: FTPConnection
    ):
        line = await connecton.readline(num_bytes=MAXLINE + 1, exit_on_eof=True)
        if len(line) > MAXLINE:
            return (
                None,
                Exception("got more than %d bytes" % MAXLINE)
            )
        
        if not line:
            return (
                None,
                Exception('End of line reached'),
            )
        
        if line[-2:] == CRLF:
            line = line[:-2]
        elif line[-1:] in CRLF:
            line = line[:-1]
        return (
            line,
            None
        )

    async def _connect_to_url_location(
        self,
        request_url: str | URL,
        connection_type: ConnectionType = 'control',
        port: int | None = None,
        control_url: FTPUrl | None = None
    ) -> Tuple[
        Exception | None,
        FTPConnection,
        FTPUrl,
        bool,
    ]:
        has_optimized_url = isinstance(request_url, URL)

        if has_optimized_url:
            parsed_url = request_url.optimized


        parsed_url = FTPUrl(
            request_url,
            family=self.address_family,
            protocol=self.address_protocol,
        )
        
        url: FTPUrl | None = None
        
        if connection_type == 'control':
            url = self._url_cache.get(parsed_url.hostname)
            dns_lock = self._dns_lock[parsed_url.hostname]
            dns_waiter = self._dns_waiters[parsed_url.hostname]
            use_ssl = 'ftps' in request_url

        else:
            url = self._url_cache.get(request_url)
            dns_lock = self._dns_lock[request_url]
            dns_waiter = self._dns_waiters[request_url]
            use_ssl = 'ftps' in control_url.full

        do_dns_lookup = (
            url is None
        ) and has_optimized_url is False

        if do_dns_lookup and dns_lock.locked() is False:

            await dns_lock.acquire()
            url = parsed_url
            await url.lookup_ftp(
                connection_type=connection_type,
                port=port,
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

        if connection_type == 'control':
            connection = self._control_connections.pop()

        else:
            connection = self._data_connections.pop()

        connection_error: Exception | None = None

        if url.address is None or connection_type == 'data':
            for address_info in url:
                try:
                    port = await connection.make_connection(
                        control_url.hostname if control_url else url.hostname, 
                        address_info,
                        url.port,
                        ssl=self._ssl_context if use_ssl else None,
                        timeout=self.timeouts.connect_timeout,
                    )

                    connection.host = address_info[-1][0]
                    connection.socket_family = url.family
                    parsed_url.address = address_info
                    parsed_url.port = port

                except Exception as err:
                    connection_error = err

        else:

            try:
                await connection.make_connection(
                    control_url.hostname if control_url else url.hostname,
                    address_info,
                    url.port,
                    ssl=self._ssl_context if use_ssl else None,
                    timeout=self.timeouts.connect_timeout,
                )

            except Exception as err:
                connection_error = err

        return (
            connection_error,
            connection,
            parsed_url,
        )