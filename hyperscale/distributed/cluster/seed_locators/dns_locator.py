"""
dns:// locator — OS-level name resolution. Returns all A/AAAA records.
"""

from __future__ import annotations

import asyncio
import socket

from .resolved_address import ResolvedAddress


class DnsLocator:
    """
    dns://manager-headless.svc.cluster.local:8080 →
        [ResolvedAddress(addr_1, 8080, "dns"), ResolvedAddress(addr_2, 8080, "dns"), ...]

    Uses the OS resolver via asyncio.get_running_loop().getaddrinfo so
    /etc/hosts, NSS, mDNS, and corporate-specific resolvers all work
    transparently per AD-52 §2 ("whatever the OS provides").

    NXDOMAIN / temporary failure → empty list (AD-52 §2: "An empty
    resolution is not an error; the locator may produce results later").
    """

    __slots__ = ("_uri", "_host", "_port")

    SCHEME_PREFIX = "dns://"

    def __init__(self, uri: str) -> None:
        if not uri.startswith(self.SCHEME_PREFIX):
            raise ValueError(
                f"DnsLocator expects {self.SCHEME_PREFIX} prefix, got {uri!r}"
            )

        address_part = uri[len(self.SCHEME_PREFIX):]
        host_part, separator, port_part = address_part.rpartition(":")
        if not separator or not host_part or not port_part:
            raise ValueError(
                f"dns:// locator {uri!r} must be dns://host:port (AD-52 §2)"
            )

        try:
            port_number = int(port_part)
        except ValueError as conversion_error:
            raise ValueError(
                f"dns:// locator {uri!r} has a non-integer port"
            ) from conversion_error

        if port_number < 1 or port_number > 65535:
            raise ValueError(f"dns:// locator {uri!r} port out of range")

        self._uri = uri
        self._host = host_part
        self._port = port_number

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def scheme(self) -> str:
        return "dns"

    async def resolve(self) -> list[ResolvedAddress]:
        running_loop = asyncio.get_running_loop()
        try:
            address_info_results = await running_loop.getaddrinfo(
                self._host,
                self._port,
                type=socket.SOCK_STREAM,
                proto=socket.IPPROTO_TCP,
            )
        except socket.gaierror:
            # NXDOMAIN, temporary failure, or "no records" — AD-52 §2
            # says this is not an error.
            return []

        # Dedupe sockaddrs by (host, port). getaddrinfo returns multiple
        # records for the same A/AAAA when the OS canonicalizes; we
        # surface each unique address once.
        seen_addresses: set[tuple[str, int]] = set()
        resolved: list[ResolvedAddress] = []
        for _family, _kind, _proto, _canonical, sockaddr in address_info_results:
            address_host = sockaddr[0]
            address_port = sockaddr[1]
            address_key = (address_host, address_port)
            if address_key in seen_addresses:
                continue
            seen_addresses.add(address_key)
            resolved.append(
                ResolvedAddress(
                    host=address_host,
                    port=address_port,
                    source_scheme="dns",
                )
            )

        return resolved

    async def refresh_required(self) -> bool:
        # DNS may change at any TTL boundary; the resolver is the OS's
        # business. Always allow a refresh.
        return True
