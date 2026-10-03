import asyncio
import posixpath
import ssl
import re
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from typing import Tuple, Literal, Any

from hyperscale.core.engines.client.shared.models import URL as FTPUrl
from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.engines.client.shared.protocols import ProtocolMap
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Data,
    File,
)
from hyperscale.core.engines.client.ftp.models.ftp import ConnectionType, CRLF
from hyperscale.core.engines.client.ftp.protocols import FTPConnection
from hyperscale.core.engines.client.ftp.protocols.tcp import MAXLINE
from .models.ftp import FTPResponse, FTPActionType



class MercurySyncFTPConnection:

    def __init__(
        self,
        pool_size: int | None = None,
        cert_path: str | None = None,
        key_path: str | None = None,
        timeouts: Timeouts | None = None,
        reset_connections: bool = False,
    ):
        self._concurrency = pool_size
        # Each engine gets its own Timeouts: a default argument would be one
        # instance shared by every engine built without timeouts.
        self.timeouts = timeouts if timeouts is not None else Timeouts()
        self.reset_connections = reset_connections

        self._cert_path = cert_path
        self._key_path = key_path
        self._ssl_context: ssl.SSLContext | None = None

        self._loop: asyncio.AbstractEventLoop | None = None


        self._dns_lock: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: list[asyncio.Future] = []

        self._client_waiters: dict[asyncio.Transport, asyncio.Future] = {}
        self._control_connections: list[FTPConnection] = []
        self._data_connections: list[FTPConnection] = []


        self._hosts: dict[str, Tuple[str, int]] = {}

        self._connections_count: dict[str, list[asyncio.Transport]] = defaultdict(list)

        self._semaphore: asyncio.Semaphore = None
        self._executor: ThreadPoolExecutor | None = None

        self._url_cache: dict[str, FTPUrl] = {}

        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.FTP]
        self._optimized: dict[str, URL | Auth | Data ] = {}

        self.address_family = address_family
        self.address_protocol = protocol
        self._is_secured: bool = False
        
        self._227_re: re.Pattern = None
        self._150_re: re.Pattern = None

    async def load_file(
        self,
        path: str,
    ):
        return await self._loop.run_in_executor(
            self._executor,
            self._upload_file,
            path,
        )

    def _upload_file(
        self,
        path: str,
    ):
        with open(path) as data:
            return data.read()
        

    async def create_account(
        self,
        url: str | URL,
        password: str,
        auth: tuple[str, str, str] | None = None,
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,

    ):
        async with self._semaphore:
            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action='CREATE_ACCOUNT',
                        auth=auth,
                        data=password,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action='CREATE_ACCOUNT',
                    error=err,
                    timings={},
                    cwd=cwd,
                )
            
    async def change_directory(
        self,
        url: str | URL,
        path: str,
        auth: tuple[str, str, str] | None = None,
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:
            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action='CHANGE_DIRECTORY',
                        auth=auth,
                        destination_path=path,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action='CHANGE_DIRECTORY',
                    error=err,
                    timings={},
                    cwd=cwd,
                )
    
    async def list_path(
        self,
        url: str | URL,
        path: str,
        auth: tuple[str, str, str] | None = None,
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:
            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action='LIST',
                        auth=auth,
                        destination_path=path,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action='LIST',
                    error=err,
                    timings={},
                    cwd=cwd,
                )
            
    async def list_directory(
        self,
        url: str | URL,
        path: str,
        auth: tuple[str, str, str] | None = None,
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:
            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action='LIST_DIRECTORY',
                        auth=auth,
                        destination_path=path,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action='LIST_DIRECTORY',
                    error=err,
                    timings={},
                    cwd=cwd,
                )
            
    async def list_details(
        self,
        url: str | URL,
        path: str,
        auth: tuple[str, str, str] | None = None,
        options: list[str] | None = None,
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:
            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action='LIST_DETAILS',
                        auth=auth,
                        destination_path=path,
                        options=options,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action='LIST_DETAILS',
                    error=err,
                    timings={},
                    cwd=cwd,
                )
            
    async def make_directory(
        self,
        url: str | URL,
        path: str,
        auth: tuple[str, str, str] | None = None,
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:
            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action='MAKE_DIRECTORY',
                        auth=auth,
                        destination_path=path,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action='MAKE_DIRECTORY',
                    error=err,
                    timings={},
                    cwd=cwd,
                )

    async def pwd(
        self,
        url: str | URL,
        auth: tuple[str, str, str] | None = None,
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:
            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action='PWD',
                        auth=auth,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action='PWD',
                    error=err,
                    timings={},
                    cwd=cwd,
                )

    async def receive(
        self,
        url: str | URL,
        path: str,
        auth: tuple[str, str, str] | None = None,
        filetype: Literal['BINARY', 'LINES'] = 'BINARY',
        chunk_size: int = 8192,
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:

            action: FTPActionType = 'RECEIVE_BINARY'
            if filetype == 'LINES':
                action = 'RECEIVE_LINES'

            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action=action,
                        auth=auth,
                        destination_path=path,
                        chunk_size=chunk_size,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action=action,
                    error=err,
                    timings={},
                    cwd=cwd,
                )
            
    async def remove(
        self,
        url: str | URL,
        path: str,
        auth: tuple[str, str, str] | None = None,
        filetype: Literal['FILE', 'DIRECTORY'] = 'FILE',
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:

            action: FTPActionType = 'REMOVE_FILE'
            if filetype == 'DIRECTORY':
                action = 'REMOVE_DIRECTORY'

            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action=action,
                        auth=auth,
                        destination_path=path,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action=action,
                    error=err,
                    timings={},
                    cwd=cwd,
                )
    
    async def rename(
        self,
        url: str | URL,
        from_name: str,
        to_name: str,
        auth: tuple[str, str, str] | None = None,
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:

            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action='RENAME',
                        auth=auth,
                        source_path=from_name,
                        destination_path=to_name,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action='RENAME',
                    error=err,
                    timings={},
                    cwd=cwd,
                )

    async def send(
        self,
        url: str | URL,
        path: str,
        data: str | Data | File | None = None,
        auth: tuple[str, str, str] | None = None,
        chunk_size: int = 8192,
        filetype: Literal['BINARY', 'LINES'] = 'BINARY',
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:

            action: FTPActionType = 'SEND_BINARY'
            if filetype == 'LINES':
                action = 'SEND_LINES'

            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        auth=auth,
                        destination_path=path,
                        data=data,
                        action=action,
                        chunk_size=chunk_size,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action=action,
                    error=err,
                    timings={},
                    cwd=cwd,
                )
            
    async def size(
        self,
        url: str | URL,
        path: str,
        auth: tuple[str, str, str] | None = None,
        timeout: int | float | None = None,
        secure_connection: bool = False,
        cwd: str | None = None,
    ):
         async with self._semaphore:

            try:

                return await asyncio.wait_for(
                    self._execute(
                        url,
                        action='SIZE',
                        auth=auth,
                        destination_path=path,
                        secure_connection=secure_connection,
                        cwd=cwd,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                return FTPResponse(
                    action='SIZE',
                    error=err,
                    timings={},
                    cwd=cwd,
                )
            
    def close(self):
        for control_connection in self._control_connections:
            if control_connection.session_open and control_connection.writer:
                control_connection.write(b'QUIT' + CRLF)

            control_connection.close()

        for data_connection in self._data_connections:
            data_connection.close()

    async def _optimize(
        self,
        optimized_param: URL | Data | File,
    ):
        if isinstance(optimized_param, URL):
            await self._optimize_url(optimized_param)

        else:
            self._optimized[optimized_param.call_name] = optimized_param

    async def _optimize_url(self, url: URL):
        try:
            if url:
                (
                    _,
                    connection,
                    _,
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(None, url),
                    timeout=self.timeouts.connect_timeout,
                )

                connection.reset()
                self._control_connections.append(connection)

            optimized_url = url.optimized

            # Plain-string requests for the same address reuse this lookup:
            # cache the resolved URL itself, under the key the connect path
            # reads, and only once the lookup actually resolved it.
            if optimized_url is not None and optimized_url.ip_addresses:
                self._url_cache[url.data] = optimized_url

            self._optimized[url.call_name] = url

        except Exception:
            pass

    async def _execute(
        self,
        url: str | URL,
        action: Literal[
            'CREATE_ACCOUNT',
            'CHANGE_DIRECTORY',
            'LIST',
            'LIST_DIRECTORY', 
            'LIST_DETAILS',
            'MAKE_DIRECTORY',
            'PWD',
            'RECEIVE_BINARY',
            'RECEIVE_LINES', 
            'REMOVE_FILE',
            'REMOVE_DIRECTORY',
            'RENAME', 
            'SEND_BINARY',
            'SEND_LINES',
            'SIZE',
        ],
        source_path: str | None = None,
        destination_path: str | None = None,
        data: str | Data | File | None = None,
        auth: tuple[str, str, str] = None,
        options: list[str] | None = None,
        chunk_size: int = 8192,
        secure_connection: bool = True,
        cwd: str | None = None,
    ):
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = {
            "request_start": None,
            "connect_start": None,
            "connect_end": None,
            "write_start": None,
            "write_end": None,
            "read_start": None,
            "read_end": None,
            "request_end": None,
        }
        timings["request_start"] = time.monotonic()

        # The request's own working directory: its relative paths resolve
        # against it here, so no session's server-side directory is relied
        # on. Absolute paths stand as given.
        if cwd is not None:
            if source_path is not None:
                source_path = posixpath.join(cwd, source_path)

            if destination_path is not None:
                destination_path = posixpath.join(cwd, destination_path)

        response_cwd = cwd
        
        control_connection: FTPConnection | None = None
        data_connection: FTPConnection | None = None
        
        try:
            
            if timings["connect_start"] is None:
                timings["connect_start"] = time.monotonic()
                
            (
                err,
                control_connection,
                url
            ) = await self._connect_to_url_location(control_connection, url)

            if err is None and control_connection.session_open is False:
                (
                    _,
                    err,
                ) = await self._get_response(control_connection)

                control_connection.session_open = err is None

            if err is None:
                async with control_connection.login_lock:
                    if control_connection.check_logged_in(auth) is False:
                        (
                            _,
                            err
                        ) = await self._login(
                            control_connection,
                            auth=auth,
                        )

                        if err is None:
                            control_connection.mark_logged_in(auth)

            if err is None and secure_connection:
                async with control_connection.secure_lock:
                    if control_connection.check_is_secure(url.hostname) is False:
                        (
                            _,
                            err
                        ) = await self._secure_connection(control_connection)

                        if err is None:
                            control_connection.mark_secure(url.hostname)

            if err:
                timings["connect_end"] = time.monotonic()

                control_connection.reset()
                self._control_connections.append(control_connection)

                return FTPResponse(
                    action=action,
                    cwd=response_cwd,
                    error=err,
                    timings=timings,
                )

            if control_connection.data_connection is None:
                control_connection.data_connection = self._data_connections.pop()
            
            if isinstance(data, Data):
                data: bytes = data.optimized

            elif isinstance(data, File):
                (
                    _,
                    data,
                    _,
                ) = data.optimized


            elif isinstance(data, str):
                data = data.encode()


            result: Any | None = None

            timings['connect_end'] = time.monotonic()

            match action:
                case 'CREATE_ACCOUNT':
                    (
                        result,
                        err,
                    ) = await self._create_account(
                        control_connection,
                        data,
                        timings=timings,
                    )
                
                case 'CHANGE_DIRECTORY':
                    (
                        result,
                        resolved_directory,
                        err,
                    ) = await self._resolve_directory(
                        control_connection,
                        destination_path,
                        timings=timings,
                    )

                    if resolved_directory is not None:
                        response_cwd = resolved_directory
                
                case 'LIST':
                    (
                        data_connection,
                        result,
                        err
                    ) = await self._list(
                        control_connection,
                        url,
                        destination_path,
                        timings=timings,
                    )

                case 'LIST_DIRECTORY':
                    (
                        data_connection,
                        result,
                        err,
                    ) = await self._list_directory(
                        control_connection,
                        url,
                        destination_path,
                        timings=timings,
                    )

                case 'LIST_DETAILS':
                    (
                        data_connection,
                        result,
                        err
                    ) = await self._list_details(
                        control_connection,
                        url,
                        destination_path,
                        options=options,
                        timings=timings,
                    )

                case 'MAKE_DIRECTORY':
                    (
                        result,
                        err,
                    ) = await self._mkdir(
                        control_connection,
                        destination_path,
                        timings=timings,
                    )

                case 'PWD':
                    (
                        result,
                        err,
                    ) = await self._pwd(
                        control_connection,
                        timings=timings,
                    )

                case 'RECEIVE_BINARY':
                    (
                        data_connection,
                        result,
                        err,
                    ) = await self._receive_binary(
                        control_connection,
                        url,
                        destination_path,
                        block_size=chunk_size,
                        timings=timings,
                    )

                case 'RECEIVE_LINES':
                    (
                        data_connection,
                        result,
                        err,
                    ) = await self._receive_lines(
                        control_connection,
                        url,
                        destination_path,
                        block_size=chunk_size,
                        timings=timings,
                    )

                case 'REMOVE_FILE':
                    (
                        result,
                        err,
                    ) = await self._remove_file(
                        control_connection,
                        destination_path,
                        timings=timings,
                    )

                case 'REMOVE_DIRECTORY':
                    (
                        result,
                        err,
                    ) = await self._remove_directory(
                        control_connection,
                        destination_path,
                        timings=timings,
                    )

                case 'RENAME':
                    (
                        result, 
                        err,
                    ) = await self._rename(
                        control_connection,
                        source_path,
                        destination_path,
                        timings=timings,
                    )

                case 'SEND_BINARY':
                    (
                        data_connection,
                        result,
                        err,
                    ) = await self._send_binary(
                        control_connection,
                        url,
                        destination_path,
                        data,
                        block_size=chunk_size,
                        timings=timings,
                    )
                
                case 'SEND_LINES':
                    (
                        data_connection,
                        result,
                        err,
                    ) = await self._send_lines(
                        control_connection,
                        url,
                        destination_path,
                        data,
                        block_size=chunk_size,
                        timings=timings,
                    )

                case 'SIZE':
                    (
                        result,
                        err,
                    ) = await self._size(
                        control_connection,
                        destination_path,
                        timings=timings,
                    )

                case _:
                    self._control_connections.append(control_connection)

                    return FTPResponse(
                        action=action,
                        cwd=response_cwd,
                        error=Exception('Unsupported action'),
                        timings=timings,
                    )
              
            timings["request_end"] = time.monotonic()  

            if err:
                control_connection.reset()

            elif control_connection.data_connection.reader is not None:
                # A passive data connection carries one transfer.
                control_connection.data_connection.reset()

            self._control_connections.append(control_connection)

            if err:
                return FTPResponse(
                    action=action,
                    cwd=response_cwd,
                    error=err,
                    data=result,
                    timings=timings,
                )
            
            return FTPResponse(
                action=action,
                cwd=response_cwd,
                data=result,
                timings=timings,
            )
        
        except (
            BaseException,
            Exception,
        ) as err:
            timings["request_end"] = time.monotonic()

            if control_connection:
                control_connection.reset()
                self._control_connections.append(control_connection)

            return FTPResponse(
                action=action,
                cwd=response_cwd,
                error=err,
                timings=timings,
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
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        if path:
            command = f"NLST {path}".encode()
        else:
            command = "NLST".encode()

        (
            connection,
            data_connection,
            lines,
            err,
        ) = await self._return(
            'TYPE A',
            connection,
            url,
            command,
            timings=timings,

        )

        if err:
            timings['read_end'] = time.monotonic()
            return (
                data_connection,
                None,
                err,
            )
        
        timings['read_end'] = time.monotonic()
        return (
            data_connection,
            lines,
            None,
        )
    
    async def _create_account(
        self,
        connection: FTPConnection,
        password: bytes,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        command = b'ACCT ' + password
        connection.write(command + CRLF)

        timings['write_end'] = time.monotonic()

        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()

        (
            result,
            err
        ) = await self._get_response(connection)

        if err:
            timings['read_end'] = time.monotonic()

            return (
                None,
                err,
            )
        
        return (
            result,
            None,
        )
    
    async def _list_directory(
        self,
        connection: FTPConnection,
        url: FTPUrl,
        path: str | None = None,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        if path:
            command = f"LIST {path}".encode()
        else:
            command = "LIST".encode()
        
        (
            connection,
            data_connection,
            lines,
            err,
        ) = await self._return(
            'TYPE A',
            connection,
            url,
            command,
            timings=timings,
        )

        if err:
            timings['read_end'] = time.monotonic()
            return (
                data_connection,
                None,
                err,
            )
        
        timings['read_end'] = time.monotonic()      
        return (
            data_connection,
            lines,
            None,
        )

    async def _list_details(
        self,
        connection: FTPConnection,
        url: FTPUrl,
        path: str | None = None,
        options: list[str] | None = None,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

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
            connection,
            data_connection,
            lines,
            err,
        ) = await self._return(
            'TYPE A',
            connection,
            url,
            command,
            timings=timings,
        )

        if err:
            timings['read_end'] = time.monotonic()
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

            entries.append(
                (name, entry),
            )

        timings['read_end'] = time.monotonic()

        return (
            data_connection,
            entries,
            None,
        )
    
    async def _mkdir(
        self,
        connection: FTPConnection,
        path: str,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()
            
        mkdir_command = f'MKD {path}'.encode()

        connection.write(mkdir_command + CRLF)

        timings['write_end'] = time.monotonic()
        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()

        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            timings['read_end'] = time.monotonic()
            return (
                None,
                err,
            )
        
        if response[:3] != b'257':
            timings['read_end'] = time.monotonic()
            return (
                None,
                Exception('Unknown error occured during MKD command')
            )
        

        elif response[3:5] != b' "':
            timings['read_end'] = time.monotonic()
            return (
                b'',
                None, # Not compliant to RFC 959, but UNIX ftpd does this
            )
        
        closing_quote = response.find(b'"', 5)
        while closing_quote != -1 and response[closing_quote + 1:closing_quote + 2] == b'"':
            closing_quote = response.find(b'"', closing_quote + 2)

        dirname = response[5:closing_quote if closing_quote != -1 else None].replace(b'""', b'"')

        timings['read_end'] = time.monotonic()
        return (
            dirname,
            None,
        )
    
    async def _rename(
        self,
        connection: FTPConnection,
        from_name: str,
        to_name: str,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        rnfr_command = f'RNFR {from_name}'.encode()
        connection.write(rnfr_command + CRLF)

        (
            _,
            err
        ) = await self._get_response(connection)

        if err:
            timings['write_end'] = time.monotonic()
            return (
                None,
                err,
            )
        
        rnto_command = f'RNTO {to_name}'.encode()
        connection.write(rnto_command + CRLF)

        timings['write_end'] = time.monotonic()
        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()


        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            timings['read_end'] = time.monotonic()
            return (
                None,
                err,
            )
        
        if response[:1] != b'2':
            timings['read_end'] = time.monotonic()

            return (
                None,
                Exception(response.decode())
            )
        
        timings['read_end'] = time.monotonic()
        return (
            response,
            None,
        )
    
    async def _pwd(
        self,
        connection: FTPConnection,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        pwd_command = b'PWD'

        connection.write(pwd_command + CRLF)

        timings['write_end'] = time.monotonic()
        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()

        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            timings['read_end'] = time.monotonic()
            return (
                None,
                err,
            )
        
        if response[:3] != b'257':
            timings['read_end'] = time.monotonic()
            return (
                None,
                Exception('Unknown error occured during PWD command')
            )
        

        elif response[3:5] != b' "':
            timings['read_end'] = time.monotonic()
            return (
                b'',
                None, # Not compliant to RFC 959, but UNIX ftpd does this
            )
        
        closing_quote = response.find(b'"', 5)
        while closing_quote != -1 and response[closing_quote + 1:closing_quote + 2] == b'"':
            closing_quote = response.find(b'"', closing_quote + 2)

        dirname = response[5:closing_quote if closing_quote != -1 else None].replace(b'""', b'"')

        timings['read_end'] = time.monotonic()
        return (
            dirname,
            None,
        )
        
    async def _remove_directory(
        self,
        connection: FTPConnection,
        path: str,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        mkdir_command = f'RMD {path}'.encode()

        connection.write(mkdir_command + CRLF)

        timings['write_end'] = time.monotonic()
        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()

        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            timings['read_end'] = time.monotonic()
            return (
                None,
                err,
            )
        
        
        if response[:1] != b'2':
            timings['read_end'] = time.monotonic()
            return (
                None,
                Exception(response.decode())
            )
        
        timings['read_end'] = time.monotonic()
        return (
            response,
            None,
        )
    
    async def _remove_file(
        self,
        connection: FTPConnection,
        path: str,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        mkdir_command = f'DELE {path}'.encode()

        connection.write(mkdir_command + CRLF)

        timings['write_end'] = time.monotonic()
        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()

        (
            response,
            err
        ) = await self._get_response(connection)

        if err:
            timings['read_end'] = time.monotonic()
            return (
                None,
                err,
            )
        

        if response[:3] in {b'250', b'200'}:
            timings['read_end'] = time.monotonic()
            return (
                response,
                None,
            )
        
        timings['read_end'] = time.monotonic()
        return (
            None,
            Exception(response.decode()),
        )
    
    async def _receive_binary(
        self,
        connection: FTPConnection,
        url: FTPUrl,
        path: str,
        block_size: int = 8192, 
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,     
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        command = f'RETR {path}'.encode()

        (
            connection,
            data_connection,
            data,
            err,
        ) = await self._return_binary(
            connection,
            url,
            command,
            block_size=block_size,
            timings=timings,
        )

        if err:
            timings['read_end'] = time.monotonic()
            return (
                data_connection,
                None,
                err,
            )
        
        timings['read_end'] = time.monotonic()
        return (
            data_connection,
            data,
            None,
        )
    
    async def _receive_lines(
        self,
        connection: FTPConnection,
        url: FTPUrl,
        path: str,
        block_size: int = 8192,   
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,     
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        command = f'RETR {path}'.encode()        

        (
            connection,
            data_connection,
            data,
            err,
        ) = await self._return(
            'TYPE A',
            connection,
            url,
            command,
            block_size=block_size,
            timings=timings,
        )

        if err:
            timings['read_end'] = time.monotonic()
            return (
                data_connection,
                None,
                err,
            )
        
        timings['read_end'] = time.monotonic()
        return (
            data_connection,
            data,
            None,
        )
    
    async def _send_binary(
        self,
        connection: FTPConnection,
        url: FTPUrl,
        path: bytes,
        data: bytes,
        block_size: int = 8192, 
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,     
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        command = f'STOR {path}'.encode()

        (
            connection,
            data_connection,
            result,
            err,
        ) = await self._store_binary(
            connection,
            url,
            command,
            data,
            block_size=block_size,
            timings=timings,
        )

        if err:
            timings['read_end'] = time.monotonic()
            return (
                data_connection,
                None,
                err,
            )
        
        timings['read_end'] = time.monotonic()
        return (
            data_connection,
            result,
            None,
        )

    async def _send_lines(
        self,
        connection: FTPConnection,
        url: FTPUrl,
        path: bytes,
        data: bytes,
        block_size: int = 8192,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,   
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        command = f'STOR {path}'.encode()

        (
            connection,
            data_connection,
            result,
            err,
        ) = await self._store_lines(
            connection,
            url,
            command,
            data,
            block_size=block_size,
            timings=timings,
        )
        
        if err:
            timings['read_end'] = time.monotonic()
            return (
                data_connection,
                None,
                err,
            )
        
        timings['read_end'] = time.monotonic()
        return (
            data_connection,
            result,
            None,
        )
    
    async def _size(
        self,
        connection: FTPConnection,
        path: str,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,   
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        if connection.transfer_type != 'TYPE I':
            connection.write(b'TYPE I' + CRLF)

            (
                _,
                err,
            ) = await self._get_response(connection)

            if err:
                timings['write_end'] = time.monotonic()
                return (
                    None,
                    err,
                )

            connection.transfer_type = 'TYPE I'

        connection.write(b'SIZE ' + path.encode() + CRLF)

        timings['write_end'] = time.monotonic()
        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()

        (
            result,
            err,
        ) = await self._get_response(connection)

        if err:
            timings['read_end'] = time.monotonic()
            return (
                None,
                err,
            )
        
        if result[:3] != b'213':
            timings['read_end'] = time.monotonic()
            return (
                None,
                Exception(result.decode()),
            )

        size = int(result[3:].strip())

        timings['read_end'] = time.monotonic()
        return (
            size,
            None,
        )
    
    async def _quit(
        self,
        connection: FTPConnection
    ):
        connection.write(b'QUIT' + CRLF)

        (
            result,
            err,
        ) = await self._get_response(connection)

        connection.close()

        if err:
            return (
                None,
                err,
            )
        
        return (
            result,
            err,
        )


    async def _store_binary(
        self,
        connection: FTPConnection,
        url: FTPUrl,
        command: bytes,
        data: bytes,
        block_size: int = 8192,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,   
    ):
        if connection.transfer_type != 'TYPE I':
            connection.write(b'TYPE I' + CRLF)

            (
                _,
                err
            ) = await self._get_response(connection)

            if err:
                timings['write_end'] = time.monotonic()
                return (
                    connection,
                    None,
                    None,
                    err,
                )

            connection.transfer_type = 'TYPE I'
        
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
            timings['write_end'] = time.monotonic()
            return (
                connection,
                data_connection,
                None,
                err,
            )
        
        total_bytes = len(data)
        for offset in range(0, total_bytes, block_size):
            chunk = data[offset: offset + block_size]
            data_connection.write(chunk)

        # The server stores the upload once its data connection closes:
        # close it gracefully, which sends what is still buffered first.
        data_connection.writer.close()

        timings['write_end'] = time.monotonic()
        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()

        (
            line,
            err,
        ) = await self._get_response(connection)

        if err:
            return (
                connection,
                data_connection,
                None,
                err,
            )
        
        if line and line[:1] != b'2':
            return (
                connection,
                data_connection,
                None,
                Exception(line.decode())
            )
        
        return (
            connection,
            data_connection,
            line,
            None,
        )

    async def _store_lines(
        self,
        connection: FTPConnection,
        url: FTPUrl,
        command: bytes,
        data: bytes,
        block_size: int = 8192,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,   
    ):
        if connection.transfer_type != 'TYPE A':
            connection.write(b'TYPE A' + CRLF)

            (
                _,
                err
            ) = await self._get_response(connection)

            if err:
                timings['write_end'] = time.monotonic()
                return (
                    connection,
                    None,
                    None,
                    err,
                )

            connection.transfer_type = 'TYPE A'
        
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
            timings['write_end'] = time.monotonic()
            return (
                connection,
                data_connection,
                None,
                err,
            )
        
        if data[-2:] != b'\r\n':
            data = data[:-1] if data[-1] in b'\r\n' else data
            data += b'\r\n'
        

        total_bytes = len(data)
        for offset in range(0, total_bytes, block_size):
            chunk = data[offset: offset + block_size]
            data_connection.write(chunk)

        # The server stores the upload once its data connection closes:
        # close it gracefully, which sends what is still buffered first.
        data_connection.writer.close()

        timings['write_end'] = time.monotonic()
        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()

        (
            line,
            err,
        ) = await self._get_response(connection)

        if err:
            return (
                connection,
                data_connection,
                None,
                err,
            )
        
        if line and line[:1] != b'2':
            return (
                connection,
                data_connection,
                None,
                Exception(line.decode())
            )
        
        return (
            connection,
            data_connection,
            line,
            None,
        )   
    
    async def _return_binary(
        self,
        connection: FTPConnection,
        url: FTPUrl,
        command: bytes,
        block_size: int = 8192,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None, 
    ):
        if connection.transfer_type != 'TYPE I':
            connection.write(b'TYPE I' + CRLF)

            (
                _,
                err
            ) = await self._get_response(connection)

            if err:
                timings['write_end'] = time.monotonic()
                return (
                    connection,
                    None,
                    None,
                    err,
                )

            connection.transfer_type = 'TYPE I'
        
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
            timings['write_end'] = time.monotonic()
            return (
                connection,
                data_connection,
                None,
                err,
            )
        
        timings['write_end'] = time.monotonic()
        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()
        
        file_bytes = bytearray()
        while data := await data_connection.read(block_size):
            file_bytes.extend(data)

        (
            line,
            err,
        ) = await self._get_response(connection)

        if err:
            return (
                connection,
                data_connection,
                None,
                err,
            )
        
        if line and line[:1] != b'2':
            return (
                connection,
                data_connection,
                None,
                Exception(line.decode())
            )
        
        return (
            connection,
            data_connection,
            file_bytes,
            None,
        )

    async def _return(
        self, 
        return_type: Literal['TYPE A', 'TYPE I'],
        connection: FTPConnection,
        url: FTPUrl,
        command: bytes,
        block_size: int = 8192,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
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

        if connection.transfer_type != return_type:
            connection.write(return_type.encode() + CRLF)

            (
                response,
                err
            ) = await self._get_response(connection)

            if err:
                timings['write_end'] = time.monotonic()
                return (
                    connection,
                    None,
                    None,
                    err,
                )

            connection.transfer_type = return_type

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
                connection,
                data_connection,
                None,
                err,
            )
        
        timings['write_end'] = time.monotonic()
        
        lines: list[bytes] = []
        raw_bytes = bytearray()

        if timings['read_start'] is None:
            timings['read_start'] = time.monotonic()


        try:
            if return_type == 'TYPE A':

                while True:

                    line = await data_connection.readline(
                        num_bytes=MAXLINE + 1, 
                        exit_on_eof=True,
                    )

                    if len(line) > MAXLINE:
                        return (
                            connection,
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

            (
                response,
                err
            ) =  await self._get_response(connection)


            if err:
                return (
                    connection,
                    data_connection,
                    None,
                    err
                )
            
            if response[0] != 50:
                return (
                    connection,
                    data_connection,
                    None,
                    Exception(response)
                )
            
            if return_type == 'TYPE A':
                return (
                    connection,
                    data_connection,
                    lines,
                    None,
                )
            
            return (
                connection,
                data_connection,
                raw_bytes,
                None,
            )
        
        except Exception as err:
            return (
                connection,
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

        
        data_connection = connection.data_connection

        try:
            await data_connection.make_connection(
                host,
                (
                    connection.socket_family,
                    socket.SOCK_STREAM,
                    0,
                    '',
                    (host, port) if connection.socket_family == socket.AF_INET else (host, port, 0, 0),
                ),
                port,
                ssl=self._ssl_context if 'ftps' in url.full else None,
                timeout=self.timeouts.connect_timeout,
            )

        except Exception as err:
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
    
    async def _get_passive_port(
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
        
    async def _resolve_directory(
        self,
        connection: FTPConnection,
        directory: str,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        """
        Resolve ``directory`` on the server and confirm it is one, leaving the
        session's directory as it was: a pooled control connection goes back
        to the pool in the directory it came out in. Returns the CWD reply,
        the resolved absolute directory, and any error.
        """
        (
            session_directory,
            err,
        ) = await self._pwd(connection, timings=timings)
        if err:
            return (None, None, err)

        (
            result,
            err,
        ) = await self._change_directory(connection, directory, timings=timings)
        if err:
            return (result, None, err)

        (
            resolved_directory,
            err,
        ) = await self._pwd(connection, timings=timings)
        if err:
            return (result, None, err)

        (
            _,
            err,
        ) = await self._change_directory(connection, session_directory.decode(), timings=timings)
        if err:
            return (result, None, err)

        return (result, resolved_directory.decode(), None)

    async def _change_directory(
        self,
        connection: FTPConnection,
        directory: str,
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "data_connect_start",
                "data_connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = None,
    ):
        
        if timings['write_start'] is None:
            timings['write_start'] = time.monotonic()

        if '\r' in directory or '\n' in directory:
            return (
                None,
                ValueError('an illegal newline character should not be contained'),
            )

        if directory == '..':
            connection.write(b'CDUP' + CRLF)

            (
                response,
                err,
            ) = await self._get_response(connection)

            if err is None and response[:3] == b'500':
                # CDUP unsupported: fall back to CWD ..
                connection.write(b'CWD ..' + CRLF)

                (
                    response,
                    err,
                ) = await self._get_response(connection)

        else:
            connection.write(b'CWD ' + (directory or '.').encode() + CRLF)

            (
                response,
                err,
            ) = await self._get_response(connection)

        timings['write_end'] = time.monotonic()
        timings['read_end'] = time.monotonic()

        if err:
            return (
                None,
                err,
            )

        if response[:1] != b'2':
            return (
                None,
                Exception(response.decode()),
            )

        return (
            response,
            None,
        )

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
        connection: FTPConnection | None,
        request_url: str | URL,
    ) -> Tuple[
        Exception | None,
        FTPConnection,
        FTPUrl,
    ]:
        has_optimized_url = isinstance(request_url, URL)

        if has_optimized_url:
            parsed_url = request_url.optimized
            requested_url = request_url.data

        else:
            parsed_url = FTPUrl(
                request_url,
                family=self.address_family,
                protocol=self.address_protocol,
            )
            requested_url = request_url

        # The address as given decides what a lookup resolves: the hostname
        # alone is shared by every port on a host, and is None for an
        # address without a scheme.
        url = self._url_cache.get(requested_url)
        dns_lock = self._dns_lock[requested_url]
        dns_waiter = self._dns_waiters[requested_url]
        use_ssl = 'ftps' in requested_url

        do_dns_lookup = (
            url is None
        ) and has_optimized_url is False

        if do_dns_lookup and dns_lock.locked() is False:
            try:
                async with dns_lock:
                    url = parsed_url
                    await url.lookup_ftp(connection_type='control')

                    self._url_cache[requested_url] = url

            finally:
                # However the lookup ended, release its waiters; after a
                # failed or cancelled lookup the next request looks up
                # again with a fresh waiter.
                if dns_waiter.done() is False:
                    dns_waiter.set_result(None)

                if requested_url not in self._url_cache:
                    del self._dns_waiters[requested_url]

        elif do_dns_lookup:
            # Shielded: a waiter's cancellation must not cancel the
            # lookup future every other waiter shares.
            await asyncio.shield(dns_waiter)
            url = self._url_cache.get(requested_url)

        elif has_optimized_url:
            url = request_url.optimized

        if connection is None:
            connection = self._control_connections.pop()

        connection_error: Exception | None = None

        try:
            # Reuses the connection's transport for this host; otherwise
            # races a new one across the host's addresses.
            address, socket_config, new_transport = await connection.connect_to_any(
                requested_url,
                url.hostname,
                url.ip_addresses,
                url.address_rotation,
                ssl=self._ssl_context if use_ssl else None,
                timeout=self.timeouts.connect_timeout,
            )

            if new_transport:
                # Resolved addresses are host strings; literal ones
                # are (host, port) tuples.
                connection.host = address if isinstance(address, str) else address[0]
                # The family of the address that connected: it picks PASV or
                # EPSV and shapes the data connection's address.
                connection.socket_family = socket_config[0]
                url.address = address
                url.socket_config = socket_config

        except asyncio.CancelledError as err:
            return (
                err,
                connection,
                parsed_url,
            )

        except Exception as err:
            connection_error = err

        try:
            return (
                connection_error,
                connection,
                parsed_url,
            )

        finally:
            # The error's traceback holds this frame: release the frame's
            # hold on the error, or the two keep each other alive as garbage.
            connection_error = None
