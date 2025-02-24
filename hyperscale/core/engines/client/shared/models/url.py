import asyncio
import functools
import socket
from socket import AddressFamily, SocketKind
from asyncio.events import get_event_loop
from ipaddress import IPv4Address, ip_address, IPv6Address
from typing import List, Tuple, Union, Literal
from urllib.parse import urlparse

import aiodns

from .ip_address_info import IpAddressInfo
from .types import SocketProtocols, SocketTypes


class URL:
    __slots__ = (
        "resolver",
        "ip_addr",
        "parsed",
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
    )

    def __init__(
        self,
        url: str,
        port: int = 80,
        family: SocketTypes = SocketTypes.DEFAULT,
        protocol: SocketProtocols = SocketProtocols.DEFAULT,
    ) -> None:
        self.resolver = aiodns.DNSResolver()
        self.parsed = urlparse(url)
        self.is_ssl = "https" in url or "wss" in url

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
        self.is_ssl = "https" in url or "wss" in url

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

    async def lookup(self):
        if self.loop is None:
            self.loop = get_event_loop()


        if self.parsed.hostname is None:
            try:

                host = self.full
                port = int(self.port)
                address_info = (host, port)

                if isinstance(ip_address(self.full), IPv6Address):
                    socket_family = socket.AF_INET6
                    address_info = (host, port, 0 , 0)

                elif isinstance(ip_address(self.full), IPv4Address):
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
            resolved = await self.resolver.gethostbyname(
                self.parsed.hostname, self.family
            )

            for address in resolved.addresses:
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
    def hostname(self):
        return self.parsed.hostname

    @hostname.setter
    def hostname(self, value):
        self.parsed = self.parsed._replace(hostname=value)

    @property
    def path(self):
        url_path = self.parsed.path
        url_query = self.parsed.query
        url_params = self.parsed.params

        if not url_path or len(url_path) == 0:
            url_path = "/"

        if url_query and len(url_query) > 0:
            url_path += f"?{self.parsed.query}"

        elif url_params and len(url_params) > 0:
            url_path += self.parsed.params

        return url_path

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
        return self.parsed.hostname

    @authority.setter
    def authority(self, value):
        self.parsed = self.parsed._replace(hostname=value)
