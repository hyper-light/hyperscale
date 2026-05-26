"""Kernel-backed port allocation with contiguous-range reservation."""

import asyncio
import socket
from dataclasses import dataclass, field

from tests.simulation.harness.errors import PortConflictError


@dataclass(slots=True)
class PortAllocator:
    """Hand out OS-selected ports the harness has verified are bindable.

    The allocator tracks every port it has issued in `_reserved` so the
    final post-teardown verification can re-bind each one and confirm the
    OS released it (catches "subprocess held the port even though the
    parent server stopped").
    """

    host: str = "127.0.0.1"
    _reserved: set[int] = field(default_factory=set, init=False)

    def reserve_pair(self) -> tuple[int, int]:
        """Reserve a (tcp, udp) pair on consecutive ports."""
        block_base = self._reserve_block(2)
        return block_base, block_base + 1

    def reserve_range(self, count: int) -> list[int]:
        """Reserve `count` consecutive ports, all verified bindable."""
        if count <= 0:
            return []
        block_base = self._reserve_block(count)
        return list(range(block_base, block_base + count))

    def reserve_worker_block(
        self,
        cores: int,
        block_size: int = 500,
        tcp_udp_offset: int = 10,
    ) -> tuple[int, int]:
        """Reserve a stride-isolated TCP/UDP pair for a worker.

        Each worker takes a contiguous ``block_size`` window so the
        derived per-subprocess UDP port (``udp + cores ** 2``) and any
        helper ports the worker pool spawns cannot collide with the
        next worker. Default 500 mirrors what the integration tests
        use and accommodates worker pools up to ~22 cores.

        Returns ``(tcp, udp)`` where ``udp = tcp + tcp_udp_offset`` —
        same convention as ``test_gate_cross_dc_dispatch.py``.
        """
        if block_size < tcp_udp_offset + cores * cores + 1:
            raise ValueError(
                f"worker block_size={block_size} is too small for "
                f"cores={cores} (need ≥ {tcp_udp_offset + cores * cores + 1})"
            )
        block_base = self._reserve_block(block_size)
        tcp = block_base
        udp = block_base + tcp_udp_offset
        return tcp, udp

    def _reserve_block(self, block_size: int) -> int:
        """Reserve a contiguous block chosen from the OS ephemeral range."""
        if block_size <= 0:
            raise ValueError("block_size must be positive")

        max_attempts = 50
        for _attempt in range(max_attempts):
            block_base = self._ephemeral_anchor()
            if block_base + block_size - 1 > 65535:
                continue

            block_ports = list(range(block_base, block_base + block_size))
            if any(port in self._reserved for port in block_ports):
                continue
            if all(self._is_bindable(p) for p in block_ports):
                for p in block_ports:
                    self._reserved.add(p)
                return block_base

        raise PortConflictError(
            f"Could not find a bindable {block_size}-port block via "
            f"kernel ephemeral probing after {max_attempts} attempts"
        )

    def _reserve_one(self) -> int:
        """Reserve one bindable OS-selected port."""
        return self._reserve_block(1)

    def _ephemeral_anchor(self) -> int:
        """Ask the kernel for a currently-free ephemeral TCP port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as anchor:
            anchor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            anchor.bind((self.host, 0))
            return int(anchor.getsockname()[1])

    def _is_bindable(self, port: int) -> bool:
        """Try to bind both TCP and UDP at this port; return True iff both succeed."""
        if not _try_bind(self.host, port, socket.SOCK_STREAM):
            return False
        if not _try_bind(self.host, port, socket.SOCK_DGRAM):
            return False
        return True

    async def verify_all_released(self, settle_seconds: float = 0.0) -> list[int]:
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

    def release(self, port: int) -> None:
        """Release one harness reservation after teardown verification."""
        self._reserved.discard(port)

    def release_all(self) -> None:
        """Release all harness reservations after teardown verification."""
        self._reserved.clear()

    def reserved_ports(self) -> list[int]:
        return sorted(self._reserved)


def _try_bind(host: str, port: int, sock_type: int) -> bool:
    """Attempt to bind a fresh socket; return True iff `bind()` succeeded.

    Sets ``SO_REUSEADDR=1`` to match the server's own bind semantics
    (mercury_sync_base_server.py uses ``SO_REUSEADDR=1`` on both its TCP
    and UDP sockets). Without this flag, harmless TIME-WAIT residue from
    a previous run's peer-to-peer manager connections — where the
    server's local port was the active closer — would surface as a
    "port still held" leak even though the next bind would succeed.
    ``SO_REUSEADDR`` does *not* mask real conflicts: a live LISTENING
    socket on the same port will still cause ``bind()`` to fail. It only
    relaxes the TIME-WAIT case, which is exactly the scenario the
    server is engineered to handle on rebind.
    """
    try:
        with socket.socket(socket.AF_INET, sock_type) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            return True
    except OSError:
        return False
