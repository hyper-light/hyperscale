"""Process-global simulation port allocation with contiguous block support."""

import asyncio
import os
import socket
from collections.abc import Iterator
from dataclasses import dataclass, field

from tests.simulation.harness.errors import PortConflictError
from tests.simulation.harness.worker_ports import worker_port_offsets


_DEFAULT_PORT_RANGE_START = 20_000
_FALLBACK_PORT_RANGE_END = 49_151
_PORT_RANGE_START_ENV = "HYPERSCALE_SIM_PORT_RANGE_START"
_PORT_RANGE_END_ENV = "HYPERSCALE_SIM_PORT_RANGE_END"

_PROCESS_RESERVED_PORTS_BY_HOST: dict[str, set[int]] = {}
_PROCESS_NEXT_PORT_BY_HOST: dict[str, int] = {}


@dataclass(slots=True)
class PortAllocator:
    """Hand out process-unique bindable ports for simulation clusters.

    Worker servers require contiguous blocks because their subprocess
    pool derives helper ports from the worker's block base. Kernel
    ``bind(port=0)`` can prove one port is free but says nothing about
    its neighboring ports, so this allocator scans a dedicated harness
    range and reserves worker spacing envelopes in a process-global
    active table. Only the concrete ports a server will bind are
    probe-checked and later verified during teardown.

    Released ports return to the process pool only after supervisor
    teardown has verified the OS can bind them again. There is no
    timed retirement: teardown is proven, not hoped for.
    """

    host: str = "127.0.0.1"
    _reserved: set[int] = field(default_factory=set, init=False)
    _logical_reserved: set[int] = field(default_factory=set, init=False)
    _span_ports_by_key: dict[tuple[int, int], set[int]] = field(
        default_factory=dict,
        init=False,
    )
    _span_actual_ports_by_key: dict[tuple[int, int], set[int]] = field(
        default_factory=dict,
        init=False,
    )
    _span_key_by_actual_port: dict[int, tuple[int, int]] = field(
        default_factory=dict,
        init=False,
    )

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

        Each worker keeps a ``block_size`` spacing envelope, but the
        allocator reserves only the concrete ports the worker runtime
        derives from that envelope:

        * TCP: ``block_base``
        * SWIM UDP: ``block_base + tcp_udp_offset``
        * RemoteGraphManager UDP: ``udp + cores ** 2``
        * Local controller UDP ports from ``WorkerLifecycleManager.get_worker_ips``

        Returns ``(tcp, udp)`` where ``udp = tcp + tcp_udp_offset``;
        same convention as ``test_gate_cross_dc_dispatch.py``.
        """
        worker_offsets = worker_port_offsets(
            cores=cores,
            tcp_udp_offset=tcp_udp_offset,
        )
        required_block_size = max(worker_offsets) + 1
        if block_size < required_block_size:
            raise ValueError(
                f"worker block_size={block_size} is too small for "
                f"cores={cores} (need >= {required_block_size})"
            )
        block_base = self._reserve_offsets(
            offsets=worker_offsets,
            span_size=block_size,
            description=f"worker {block_size}-port spacing block",
        )
        tcp = block_base
        udp = block_base + tcp_udp_offset
        return tcp, udp

    def _reserve_block(self, block_size: int) -> int:
        """Reserve a contiguous bindable block from the harness range."""
        if block_size <= 0:
            raise ValueError("block_size must be positive")
        return self._reserve_offsets(
            offsets=tuple(range(block_size)),
            span_size=block_size,
            description=f"{block_size}-port block",
        )

    def _reserve_offsets(
        self,
        offsets: tuple[int, ...],
        span_size: int,
        description: str,
    ) -> int:
        """Reserve bindable ports at ``offsets`` from one block base."""
        _validate_offsets(
            offsets=offsets,
            span_size=span_size,
            description=description,
        )
        range_start, range_end = _port_range()
        range_size = range_end - range_start + 1
        if span_size > range_size:
            raise PortConflictError(
                f"Cannot reserve {description} from harness range "
                f"[{range_start}, {range_end}]"
            )

        process_reserved = _PROCESS_RESERVED_PORTS_BY_HOST.setdefault(
            self.host,
            set(),
        )
        start_port = _PROCESS_NEXT_PORT_BY_HOST.setdefault(self.host, range_start)
        for block_base in _candidate_block_bases(
            start_port=start_port,
            span_size=span_size,
            range_start=range_start,
            range_end=range_end,
        ):
            logical_ports = range(block_base, block_base + span_size)
            block_ports = [block_base + offset for offset in offsets]
            if any(
                port in self._logical_reserved or port in process_reserved
                for port in logical_ports
            ):
                continue
            if all(self._is_bindable(p) for p in block_ports):
                span_key = (block_base, span_size)
                actual_port_set = set(block_ports)
                logical_port_set = set(range(block_base, block_base + span_size))
                self._span_ports_by_key[span_key] = logical_port_set
                self._span_actual_ports_by_key[span_key] = actual_port_set
                for p in actual_port_set:
                    self._reserved.add(p)
                    self._span_key_by_actual_port[p] = span_key
                self._logical_reserved.update(logical_port_set)
                process_reserved.update(logical_port_set)
                _PROCESS_NEXT_PORT_BY_HOST[self.host] = _next_candidate_after(
                    block_base + span_size,
                    range_start=range_start,
                    range_end=range_end,
                )
                return block_base

        raise PortConflictError(
            f"Could not find bindable {description} in harness "
            f"range [{range_start}, {range_end}]"
        )

    def _reserve_one(self) -> int:
        """Reserve one bindable port from the harness range."""
        return self._reserve_block(1)

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
        span_key = self._span_key_by_actual_port.pop(port, None)
        process_reserved = _PROCESS_RESERVED_PORTS_BY_HOST.get(self.host)
        if span_key is None:
            self._logical_reserved.discard(port)
            if process_reserved is not None:
                process_reserved.discard(port)
            return

        actual_ports = self._span_actual_ports_by_key.get(span_key)
        if actual_ports is not None:
            actual_ports.discard(port)
            if actual_ports:
                return

        self._span_actual_ports_by_key.pop(span_key, None)
        logical_ports = self._span_ports_by_key.pop(span_key, set())
        self._logical_reserved.difference_update(logical_ports)
        if process_reserved is not None:
            process_reserved.difference_update(logical_ports)

    def release_all(self) -> None:
        """Release all harness reservations after teardown verification."""
        process_reserved = _PROCESS_RESERVED_PORTS_BY_HOST.get(self.host)
        if process_reserved is not None:
            process_reserved.difference_update(self._logical_reserved)
        self._reserved.clear()
        self._logical_reserved.clear()
        self._span_ports_by_key.clear()
        self._span_actual_ports_by_key.clear()
        self._span_key_by_actual_port.clear()

    def reserved_ports(self) -> list[int]:
        return sorted(self._reserved)


def _try_bind(host: str, port: int, sock_type: int) -> bool:
    """Attempt to bind a fresh socket; return True iff `bind()` succeeded.

    TCP probes set ``SO_REUSEADDR=1`` to match server bind semantics and
    ignore harmless TIME-WAIT residue from previous peer connections.
    UDP has no TIME-WAIT state, and ``SO_REUSEADDR`` can mask a live UDP
    owner on some platforms, so UDP probes bind without reuse flags.
    """
    try:
        with socket.socket(socket.AF_INET, sock_type) as sock:
            if sock_type == socket.SOCK_STREAM:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            return True
    except OSError:
        return False


def _port_range() -> tuple[int, int]:
    """Return the configured harness port range."""
    linux_ephemeral_start = _linux_ephemeral_range_start()
    range_start = _read_port_env(
        name=_PORT_RANGE_START_ENV,
        default=_default_port_range_start(linux_ephemeral_start),
    )
    range_end = _read_port_env(
        name=_PORT_RANGE_END_ENV,
        default=_default_port_range_end(
            range_start=range_start,
            linux_ephemeral_start=linux_ephemeral_start,
        ),
    )
    if range_start < 1024:
        raise PortConflictError(
            f"{_PORT_RANGE_START_ENV} must be >= 1024; got {range_start}"
        )
    if range_end > 65535:
        raise PortConflictError(
            f"{_PORT_RANGE_END_ENV} must be <= 65535; got {range_end}"
        )
    if range_start > range_end:
        raise PortConflictError(
            f"Invalid harness port range [{range_start}, {range_end}]"
        )
    if linux_ephemeral_start is not None and range_end >= linux_ephemeral_start:
        raise PortConflictError(
            "Harness port range overlaps Linux ephemeral ports: "
            f"[{range_start}, {range_end}] reaches {linux_ephemeral_start}. "
            f"Set {_PORT_RANGE_END_ENV} below {linux_ephemeral_start}."
        )
    return range_start, range_end


def _read_port_env(name: str, default: int) -> int:
    """Read an integer port-range environment variable."""
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError as error:
        raise PortConflictError(f"{name} must be an integer port") from error


def _default_port_range_start(linux_ephemeral_start: int | None) -> int:
    """Return a default lower bound outside the OS ephemeral range."""
    if (
        linux_ephemeral_start is not None
        and linux_ephemeral_start <= _DEFAULT_PORT_RANGE_START
    ):
        return 1024
    return _DEFAULT_PORT_RANGE_START


def _default_port_range_end(
    *,
    range_start: int,
    linux_ephemeral_start: int | None,
) -> int:
    """Return a best-effort non-ephemeral upper bound for harness ports."""
    if linux_ephemeral_start is not None and linux_ephemeral_start > range_start:
        return min(_FALLBACK_PORT_RANGE_END, linux_ephemeral_start - 1)
    return _FALLBACK_PORT_RANGE_END


def _linux_ephemeral_range_start() -> int | None:
    """Read Linux's auto-assigned ephemeral range start when available."""
    try:
        with open("/proc/sys/net/ipv4/ip_local_port_range") as port_range_file:
            range_start_text, _range_end_text = port_range_file.read().split()
            return int(range_start_text)
    except (OSError, ValueError):
        return None


def _candidate_block_bases(
    *,
    start_port: int,
    span_size: int,
    range_start: int,
    range_end: int,
) -> Iterator[int]:
    """Return each candidate block base once, wrapping at range end."""
    highest_base = range_end - span_size + 1
    normalized_start = min(max(start_port, range_start), highest_base)
    yield from range(normalized_start, highest_base + 1)
    yield from range(range_start, normalized_start)


def _next_candidate_after(
    port: int,
    *,
    range_start: int,
    range_end: int,
) -> int:
    """Return the next scan cursor within the harness port range."""
    if port > range_end:
        return range_start
    return port


def _validate_offsets(
    *,
    offsets: tuple[int, ...],
    span_size: int,
    description: str,
) -> None:
    """Validate a set of concrete port offsets inside a logical span."""
    if span_size <= 0:
        raise ValueError("span_size must be positive")
    if not offsets:
        raise ValueError(f"{description} must reserve at least one concrete port")

    invalid_offsets = [
        offset for offset in offsets if offset < 0 or offset >= span_size
    ]
    if invalid_offsets:
        raise ValueError(
            f"{description} has offsets outside span_size={span_size}: "
            f"{invalid_offsets}"
        )
