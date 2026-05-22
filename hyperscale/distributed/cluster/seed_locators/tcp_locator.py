"""
tcp:// locator — a literal address. Resolution is a no-op string parse.
"""

from __future__ import annotations

from .resolved_address import ResolvedAddress


class TcpLocator:
    """
    tcp://10.0.5.7:8080 → [ResolvedAddress("10.0.5.7", 8080, "tcp")].

    The host part is passed through unchanged; if it is a DNS name (not
    an IP literal), late resolution at connection time uses the OS
    resolver. Port is required and validated.
    """

    __slots__ = ("_uri", "_host", "_port")

    SCHEME_PREFIX = "tcp://"

    def __init__(self, uri: str) -> None:
        if not uri.startswith(self.SCHEME_PREFIX):
            raise ValueError(
                f"TcpLocator expects {self.SCHEME_PREFIX} prefix, got {uri!r}"
            )

        address_part = uri[len(self.SCHEME_PREFIX):]
        host_part, separator, port_part = address_part.rpartition(":")
        if not separator or not host_part or not port_part:
            raise ValueError(
                f"tcp:// locator {uri!r} must be tcp://host:port (AD-52 §2)"
            )

        try:
            port_number = int(port_part)
        except ValueError as conversion_error:
            raise ValueError(
                f"tcp:// locator {uri!r} has a non-integer port"
            ) from conversion_error

        if port_number < 1 or port_number > 65535:
            raise ValueError(f"tcp:// locator {uri!r} port out of range")

        self._uri = uri
        self._host = host_part
        self._port = port_number

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def scheme(self) -> str:
        return "tcp"

    async def resolve(self) -> list[ResolvedAddress]:
        return [ResolvedAddress(host=self._host, port=self._port, source_scheme="tcp")]

    async def refresh_required(self) -> bool:
        # Literal addresses never change.
        return False
