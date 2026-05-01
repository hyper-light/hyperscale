"""
Port allocation with try-bind probing and contiguous-range reservation.

Probes by binding ephemeral sockets before handing ports out, so the
harness fails fast and clearly when prior-run zombies are squatting on
ports — rather than letting a server later fail with an opaque
`Address already in use`.
"""

import asyncio
import socket
from dataclasses import dataclass, field

from tests.simulation.harness.errors import PortConflictError


@dataclass(slots=True)
class PortAllocator:
    """Hand out ports the harness has verified are bindable.

    The allocator tracks every port it has issued in `_reserved` so the
    final post-teardown verification can re-bind each one and confirm the
    OS released it (catches "subprocess held the port even though the
    parent server stopped").
    """

    host: str = "127.0.0.1"
    base_port: int = 9000
    _next_port: int = field(init=False)
    _reserved: set[int] = field(default_factory=set, init=False)

    def __post_init__(self) -> None:
        self._next_port = self.base_port

    def reserve_pair(self) -> tuple[int, int]:
        """Reserve a (tcp, udp) pair on consecutive ports."""
        return (self._reserve_one(), self._reserve_one())

    def reserve_range(self, count: int) -> list[int]:
        """Reserve `count` consecutive ports, all verified bindable."""
        if count <= 0:
            return []
        return [self._reserve_one() for _ in range(count)]

    def _reserve_one(self) -> int:
        """Probe upward from `_next_port` until one binds; reserve and return it."""
        attempts = 0
        max_attempts = 10_000
        while attempts < max_attempts:
            candidate = self._next_port
            self._next_port += 1
            attempts += 1
            if candidate in self._reserved:
                continue
            if self._is_bindable(candidate):
                self._reserved.add(candidate)
                return candidate
        raise PortConflictError(
            f"Could not find a bindable port after {max_attempts} attempts"
            f" starting from {self.base_port}"
        )

    def _is_bindable(self, port: int) -> bool:
        """Try to bind both TCP and UDP at this port; return True iff both succeed."""
        if not _try_bind(self.host, port, socket.SOCK_STREAM):
            return False
        if not _try_bind(self.host, port, socket.SOCK_DGRAM):
            return False
        return True

    async def verify_all_released(self, settle_seconds: float = 0.5) -> list[int]:
        """Verify every reserved port is bindable again.

        Returns the list of ports still held; an empty list means clean
        teardown. Sleeps briefly first to give the OS a moment to drain
        sockets after server shutdown.
        """
        await asyncio.sleep(settle_seconds)
        held: list[int] = []
        for port in sorted(self._reserved):
            if not self._is_bindable(port):
                held.append(port)
        return held

    def reserved_ports(self) -> list[int]:
        return sorted(self._reserved)


def _try_bind(host: str, port: int, sock_type: int) -> bool:
    """Attempt to bind a fresh socket; return True iff `bind()` succeeded.

    Uses `SO_REUSEADDR` *off* deliberately — we want to detect ports the
    OS still considers held by another process.
    """
    try:
        with socket.socket(socket.AF_INET, sock_type) as sock:
            sock.bind((host, port))
            return True
    except OSError:
        return False
