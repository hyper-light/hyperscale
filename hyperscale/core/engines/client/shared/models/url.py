import asyncio
import functools
import itertools
import socket
from socket import AddressFamily, SocketKind
from asyncio.events import get_event_loop
from ipaddress import IPv4Address, ip_address, IPv6Address
from typing import List, Tuple, Union, Literal
from urllib.parse import ParseResult, urlparse, urlsplit

import aiodns

from .ip_address_info import IpAddressInfo
from .types import SocketProtocols, SocketTypes


TLS_SCHEMES = frozenset(("https", "wss"))
WEBSOCKET_SCHEMES = frozenset(("ws", "wss"))
# The port each scheme's requests go to when the address names none
# (RFC 9110 4.2.1-4.2.2, RFC 6455 3).
DEFAULT_PORTS = {"http": 80, "https": 443, "ws": 80, "wss": 443}


def is_ip_address(host: str) -> bool:
    """Whether ``host`` is an IP address, needing no lookup, rather than a name."""
    try:
        ip_address(host)

    except ValueError:
        return False

    return True


def split_host_port(address: str, default_port: int) -> Tuple[str, int]:
    """
    The host and port of an address given without a scheme: a bare IP
    address (IPv6 included), ``host``, ``host:port`` or ``[IPv6]:port``.
    """
    if is_ip_address(address):
        return address, default_port

    parsed = urlsplit(f"//{address}")
    if parsed.hostname is None:
        raise ValueError(f"No host in address {address!r}")

    return parsed.hostname, parsed.port if parsed.port is not None else default_port


def request_target_path(parsed: ParseResult) -> str:
    """The path, with its ;params and its query, a request names (RFC 3986 3.3, 3.4)."""
    url_path = parsed.path or "/"

    if parsed.params:
        url_path = f"{url_path};{parsed.params}"

    if parsed.query:
        return f"{url_path}?{parsed.query}"

    return url_path


class URL:
    __slots__ = (
        "ip_addr",
        "_parsed",
        "_hostname",
        "_path",
        "target",
        "is_ssl",
        "port",
        "full",
        "has_ip_addr",
        "socket_config",
        "family",
        "protocol",
        "loop",
        "ip_addresses",
        "address",
        "address_rotation",
        "http2_pseudo_headers",
    )

    def __init__(
        self,
        url: str,
        port: int = 80,
        family: SocketTypes = SocketTypes.DEFAULT,
        protocol: SocketProtocols = SocketProtocols.DEFAULT,
    ) -> None:
        self.parsed = urlparse(url)

        if self.is_ssl:
            port = 443

        if family is None:
            family = SocketTypes.DEFAULT

        self.port = self.parsed.port if self.parsed.port else port
        self.full = url
        self.has_ip_addr = False
        self.family = family
        self.protocol = protocol
        self.loop = None
        self.ip_addresses: List[Tuple[
            Tuple[str, int], 
            Tuple[AddressFamily, SocketKind, int, str, Tuple[str, int]]
        ]] = []
        self.address: Union[str, None] = None
        self.socket_config: Union[Tuple[str, int], Tuple[str, int, int, int], None] = (
            None
        )
        # Each new connection to this host starts at the next address, so a
        # pool's connections spread across all of them.
        self.address_rotation = itertools.count()

    async def replace(self, url: str):
        self.full = url
        self.params = urlparse(url)

    def __iter__(self):
        for ip_info in self.ip_addresses:
            yield ip_info

    def update(
        self,
        url: str,
        port: int = 80,
        family: SocketTypes = SocketTypes.DEFAULT,
        protocol: SocketProtocols = SocketProtocols.DEFAULT,
    ):
        self.parsed = urlparse(url)

        if self.is_ssl:
            port = 443

        self.port = self.parsed.port if self.parsed.port else port
        self.full = url
        self.has_ip_addr = False
        self.socket_config: Union[Tuple[str, int], Tuple[str, int, int, int], None] = (
            None
        )
        self.family = family
        self.protocol = protocol
        self.loop = None
        self.ip_addresses: List[Tuple[
            Tuple[str, int], 
            Tuple[AddressFamily, SocketKind, int, str, Tuple[str, int]]
        ]] = []
        self.address: Union[str, None] = None

    async def lookup_ssh(self):

        port = self.parsed.port

        if not port:
            port = 22

        self.port = port

        if self.loop is None:
            self.loop = get_event_loop()

        if self.parsed.hostname is None:
            try:

                # No scheme: a bare IP address (IPv6 included), host,
                # host:port or [IPv6]:port.
                host, port = split_host_port(self.full, port)
                self.port = port

                address_info = (host, port)

                if host == 'localhost':
                    socket_family = socket.AF_INET

                if isinstance(ip_address(host), IPv6Address):
                    socket_family = socket.AF_INET6
                    address_info = (host, port, 0 , 0)

                elif isinstance(ip_address(host), IPv4Address):
                    socket_family = socket.AF_INET

                address = (host, port)

                
                self.ip_addresses = [
                    (
                        address,
                        (
                            socket_family,
                            socket.SOCK_STREAM,
                            0,
                            "",
                            address_info,
                        ),
                    )
                ]


            except Exception:

                
                # No scheme: a bare IP address (IPv6 included), host,
                # host:port or [IPv6]:port.
                host, port = split_host_port(self.full, port)
                self.port = port
                    
                address = (host, port)

                self.ip_addresses = [
                    (
                        address,
                        (
                            socket.AF_INET,
                            socket.SOCK_STREAM,
                            0,
                            "",
                            address_info,
                        ),
                    )
                ]

        else:
            # An IP address needs no lookup, and DNS rejects IPv6 ones
            # ("Misformatted domain name") unless asked for AAAA records.
            hostname = self.parsed.hostname
            if is_ip_address(hostname):
                addresses = [hostname]

            else:
                # Each resolver runs its own thread: create one per lookup,
                # closed with it, never one per URL a request builds.
                async with aiodns.DNSResolver() as resolver:
                    resolved = await resolver.getaddrinfo(hostname, family=self.family)

                addresses = [node.addr[0].decode() for node in resolved.nodes]

            for address in addresses:
                if isinstance(ip_address(address), IPv4Address):
                    socket_family = socket.AF_INET
                    address_info = (address, self.port)

                else:
                    socket_family = socket.AF_INET6
                    address_info = (address, self.port, 0, 0)

                self.ip_addresses.append(
                    (
                        address,
                        (
                            socket_family,
                            socket.SOCK_STREAM,
                            0,
                            "",
                            address_info,
                        ),
                    )
                )

    async def lookup_ftp(
        self,
        connection_type: Literal['control', 'data'] = 'control',
        port: int | None = None,
    ):
        
        if port is None:
            # The address's own port, when it names one.
            port = self.parsed.port

        if port is None and connection_type == 'control' and 'sftp' in self.full:
            port = 22

        elif port is None and connection_type == 'control':
            port = 21
  
        self.port = port
        if self.loop is None:
            self.loop = get_event_loop()


        if self.parsed.hostname is None:
            try:

                # No scheme: a bare IP address (IPv6 included), host:port
                # or [IPv6]:port.
                host, port = split_host_port(self.full, int(self.port))
                self.port = port
                address_info = (host, port)

                if isinstance(ip_address(host), IPv6Address):
                    socket_family = socket.AF_INET6
                    address_info = (host, port, 0 , 0)

                elif isinstance(ip_address(host), IPv4Address):
                    socket_family = socket.AF_INET

                address = (host, port)

                
                self.ip_addresses = [
                    (
                        address,
                        (
                            socket_family,
                            socket.SOCK_STREAM,
                            0,
                            "",
                            address_info,
                        ),
                    )
                ]


            except Exception as parse_error:
                raise parse_error

        else:
            # An IP address needs no lookup, and DNS rejects IPv6 ones
            # ("Misformatted domain name") unless asked for AAAA records.
            hostname = self.parsed.hostname
            if is_ip_address(hostname):
                addresses = [hostname]

            else:
                # Each resolver runs its own thread: create one per lookup,
                # closed with it, never one per URL a request builds.
                async with aiodns.DNSResolver() as resolver:
                    resolved = await resolver.getaddrinfo(hostname, family=self.family)

                addresses = [node.addr[0].decode() for node in resolved.nodes]

            for address in addresses:
                if isinstance(ip_address(address), IPv4Address):
                    socket_family = socket.AF_INET
                    address_info = (address, self.port)

                else:
                    socket_family = socket.AF_INET6
                    address_info = (address, self.port, 0, 0)

                self.ip_addresses.append(
                    (
                        address,
                        (
                            socket_family,
                            socket.SOCK_STREAM,
                            0,
                            "",
                            address_info,
                        ),
                    )
                )

    async def lookup_smtp(
        self,
        server: str,
        loop: asyncio.AbstractEventLoop,
        connection_type: Literal['insecure', 'ssl', 'tls'] = 'tls',
    ):
        
        port = 587
        match connection_type:
            case "insecure":
                port = 25

            case "ssl":
                port = 465

            case "tls":
                port = 587

            case _:
                port = None
                
        self.ip_addresses = []

        if port:
            self.ip_addresses = await loop.run_in_executor(
                None,
                functools.partial(
                    socket.getaddrinfo,
                    server, 
                    port, 
                    0, 
                    socket.SOCK_STREAM
                )
            )

        else:
            for port in [587, 465, 25]:
                try:

                    self.ip_addresses.extend(
                        await loop.run_in_executor(
                            None,
                            functools.partial(
                                socket.getaddrinfo,
                                server, 
                                port, 
                                0, 
                                socket.SOCK_STREAM
                            )
                        )
                    )

                except Exception:
                    raise Exception(f'Invalid server {server}')

    async def lookup(self):
        if self.loop is None:
            self.loop = get_event_loop()

        if (hostname := self.parsed.hostname) is None:
            # No scheme: a bare IP address (IPv6 included), host, host:port
            # or [IPv6]:port.
            hostname, self.port = split_host_port(self.full, int(self.port))

        # An IP address needs no lookup, and DNS rejects IPv6 ones
        # ("Misformatted domain name") unless asked for AAAA records.
        if is_ip_address(hostname):
            addresses = [hostname]

        else:
            # Each resolver runs its own thread: create one per lookup,
            # closed with it, never one per URL a request builds.
            async with aiodns.DNSResolver() as resolver:
                resolved = await resolver.getaddrinfo(hostname, family=self.family)

            addresses = [node.addr[0].decode() for node in resolved.nodes]

        socket_kind = self.protocol if self.protocol is not None else socket.SOCK_STREAM
        self.ip_addresses = [
            (
                address,
                (socket.AF_INET, socket_kind, 0, "", (address, self.port))
                if isinstance(ip_address(address), IPv4Address)
                else (socket.AF_INET6, socket_kind, 0, "", (address, self.port, 0, 0)),
            )
            for address in addresses
        ]

    @property
    def params(self):
        return self.parsed.params

    @params.setter
    def params(self, value: str):
        self.parsed = self.parsed._replace(params=value)

    @property
    def scheme(self):
        return self.parsed.scheme

    @scheme.setter
    def scheme(self, value):
        self.parsed = self.parsed._replace(scheme=value)

    @property
    def parsed(self) -> ParseResult:
        return self._parsed

    @parsed.setter
    def parsed(self, value: ParseResult) -> None:
        # Every request reads the hostname and path; derive them once per
        # parse rather than re-splitting the netloc on each read.
        self._parsed = value
        self._hostname = value.hostname
        self._path = request_target_path(value)
        # TLS follows the scheme alone: a path that merely contains "https"
        # is not a TLS address.
        self.is_ssl = value.scheme in TLS_SCHEMES
        # The connection key: scheme and authority (host and port), or the
        # whole address when it has no scheme -- and for a WebSocket its
        # resource too, since each WebSocket is a session with one resource.
        # Every lookup cache and pooled transport keys on it; built once
        # here, never per request.
        if value.scheme in WEBSOCKET_SCHEMES:
            self.target = (value.scheme, value.netloc, self._path)

        else:
            self.target = (value.scheme, value.netloc or value.geturl())

        # The HTTP/2 engine's encoded :authority, :scheme and :path, built on
        # its first request to this address; a new parse discards them.
        self.http2_pseudo_headers = None

    @property
    def hostname(self):
        return self._hostname

    @hostname.setter
    def hostname(self, value):
        self.parsed = self.parsed._replace(hostname=value)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self.parsed = self.parsed._replace(path=value)

    @property
    def query(self):
        return self.parsed.query

    @query.setter
    def query(self, value):
        self.parsed = self.parsed._replace(query=value)

    @property
    def authority(self):
        return self._hostname

    @authority.setter
    def authority(self, value):
        self.parsed = self.parsed._replace(hostname=value)
