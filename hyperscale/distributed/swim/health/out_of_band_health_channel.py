"""
Out-of-Band Health Channel for High-Priority SWIM Probes (Phase 6.3).

When nodes are overloaded, regular SWIM probes may be delayed due to queue
buildup. This channel provides a separate, lightweight path for health checks
that bypasses the normal message queue.

Key design decisions:
1. Uses a dedicated UDP socket for health messages only
2. Minimal message format for fast processing
3. Separate receive loop that processes immediately (no queueing)
4. Rate-limited to prevent this channel from becoming a DoS vector

Use cases:
1. Quick liveness check for suspected-dead nodes
2. Health verification before marking a node as dead
3. Cross-cluster health probes that need guaranteed low latency

Integration:
- HealthAwareServer can optionally enable OOB channel
- OOB probes are sent when normal probes fail or timeout
- OOB channel is checked before declaring a node dead

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
import socket
from dataclasses import dataclass, field
from typing import Callable
from hyperscale.distributed.swim.core.protocols import LoggerProtocol
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.logging.hyperscale_logging_models import ServerError

from .oob_health_channel_config import MAX_OOB_MESSAGE_SIZE
from .oob_health_channel_config import OOB_MAX_PROBES_PER_SECOND
from .oob_health_channel_config import OOB_PROBE_COOLDOWN
from .oob_health_channel_config import OOBHealthChannelConfig
from .oob_probe_result import OOBProbeResult

_DEFAULT_CLOCK: Clock = RealClock()

# Message format: single byte type + payload
OOB_PROBE = b"\x01"  # Health probe request

OOB_ACK = b"\x02"  # Health probe acknowledgment

OOB_NACK = b"\x03"  # Health probe negative acknowledgment (overloaded)


@dataclass(slots=True)
class OutOfBandHealthChannel:
    """
    Out-of-band health channel for high-priority probes.

    This provides a separate UDP channel for health checks that need to
    bypass the normal SWIM message queue. It's particularly useful when
    probing nodes that might be overloaded.

    Usage:
        channel = OutOfBandHealthChannel(
            host="0.0.0.0",
            base_port=8000,
        )
        await channel.start()

        # Send probe
        result = await channel.probe(("192.168.1.1", 8100))
        if result.success:
            print(f"Node alive, latency: {result.latency_ms}ms")
        elif result.is_overloaded:
            print("Node alive but overloaded")

        await channel.stop()
    """

    host: str
    base_port: int
    config: OOBHealthChannelConfig = field(default_factory=OOBHealthChannelConfig)

    # Internal state
    _socket: socket.socket | None = field(default=None, repr=False)
    _receive_task: asyncio.Task | None = field(default=None, repr=False)
    _running: bool = False

    # Pending probes awaiting response
    _pending_probes: dict[tuple[str, int], asyncio.Future] = field(default_factory=dict)

    # Rate limiting
    _last_probe_time: dict[tuple[str, int], float] = field(default_factory=dict)
    _global_probe_count: int = 0
    _global_probe_window_start: float = field(default_factory=lambda: _DEFAULT_CLOCK.monotonic())

    # Callback for when we receive a probe (to generate response)
    _is_overloaded: Callable[[], bool] | None = None

    # Statistics
    _probes_sent: int = 0
    _probes_received: int = 0
    _acks_sent: int = 0
    _nacks_sent: int = 0
    _timeouts: int = 0
    _reply_send_failures: int = 0

    _logger: LoggerProtocol | None = None
    _node_id: str = ""

    @property
    def port(self) -> int:
        """Get the OOB channel port."""
        return self.base_port + self.config.port_offset

    def set_overload_checker(self, checker: Callable[[], bool]) -> None:
        self._is_overloaded = checker

    def set_logger(self, logger: LoggerProtocol, node_id: str) -> None:
        self._logger = logger
        self._node_id = node_id

    async def _log_error(self, message: str) -> None:
        if self._logger:

            await self._logger.log(
                ServerError(
                    message=message,
                    node_host=self.host,
                    node_port=self.port,
                    node_id=self._node_id,
                )
            )

    async def start(self) -> None:
        """Start the OOB health channel."""
        if self._running:
            return

        # Create non-blocking UDP socket
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setblocking(False)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self._socket.bind((self.host, self.port))
        except OSError as e:
            self._socket.close()
            self._socket = None
            raise RuntimeError(
                f"Failed to bind OOB channel on {self.host}:{self.port}: {e}"
            )

        self._running = True
        # Phase 6b: explicit ``loop.create_task`` so the task binds to
        # the loop the OOB channel was started on rather than implicitly
        # going through ``get_running_loop`` at task-creation time.
        self._receive_task = asyncio.get_running_loop().create_task(
            self._receive_loop()
        )

    async def stop(self) -> None:
        """Stop the OOB health channel."""
        self._running = False

        if self._receive_task:
            self._receive_task.cancel()
            cancels_requested_before_wait = asyncio.current_task().cancelling()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                # The task we cancelled ended; a cancel aimed at this task
                # while it waited goes on.
                if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                    raise
            self._receive_task = None

        # Cancel pending probes
        for future in self._pending_probes.values():
            if not future.done():
                future.cancel()
        self._pending_probes.clear()

        if self._socket:
            self._socket.close()
            self._socket = None

    async def probe(self, target: tuple[str, int]) -> OOBProbeResult:
        """
        Send an out-of-band probe to a target.

        Args:
            target: (host, port) of the target's OOB channel

        Returns:
            OOBProbeResult with success/failure and latency
        """
        if not self._running or not self._socket:
            return OOBProbeResult(
                target=target,
                success=False,
                is_overloaded=False,
                latency_ms=0.0,
                error="OOB channel not running",
            )

        # Rate limiting checks
        if not self._check_rate_limit(target):
            return OOBProbeResult(
                target=target,
                success=False,
                is_overloaded=False,
                latency_ms=0.0,
                error="Rate limited",
            )

        # Create future for response
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_probes[target] = future

        start_time = _DEFAULT_CLOCK.monotonic()

        try:
            # Send probe
            message = OOB_PROBE + f"{self.host}:{self.port}".encode()
            await asyncio.get_event_loop().sock_sendto(
                self._socket,
                message,
                target,
            )
            self._probes_sent += 1
            self._last_probe_time[target] = _DEFAULT_CLOCK.monotonic()

            # Wait for response
            try:
                response = await _DEFAULT_CLOCK.wait_for(
                    future,
                    timeout=self.config.probe_timeout_seconds,
                )

                latency = (_DEFAULT_CLOCK.monotonic() - start_time) * 1000
                is_overloaded = response == OOB_NACK

                return OOBProbeResult(
                    target=target,
                    success=True,
                    is_overloaded=is_overloaded,
                    latency_ms=latency,
                )

            except asyncio.TimeoutError:
                self._timeouts += 1
                return OOBProbeResult(
                    target=target,
                    success=False,
                    is_overloaded=False,
                    latency_ms=(_DEFAULT_CLOCK.monotonic() - start_time) * 1000,
                    error="Timeout",
                )

            except asyncio.CancelledError:
                # Probe was cancelled (e.g., during shutdown)
                # Return graceful failure instead of propagating
                return OOBProbeResult(
                    target=target,
                    success=False,
                    is_overloaded=False,
                    latency_ms=(_DEFAULT_CLOCK.monotonic() - start_time) * 1000,
                    error="Cancelled",
                )

        except asyncio.CancelledError:
            # Cancelled during send - graceful failure
            return OOBProbeResult(
                target=target,
                success=False,
                is_overloaded=False,
                latency_ms=(_DEFAULT_CLOCK.monotonic() - start_time) * 1000,
                error="Cancelled",
            )

        except Exception as e:
            return OOBProbeResult(
                target=target,
                success=False,
                is_overloaded=False,
                latency_ms=(_DEFAULT_CLOCK.monotonic() - start_time) * 1000,
                error=str(e),
            )

        finally:
            self._pending_probes.pop(target, None)

    async def _receive_loop(self) -> None:
        """Receive loop for OOB messages."""
        loop = asyncio.get_event_loop()

        current_addr: tuple[str, int] | None = None
        current_msg_type: bytes | None = None

        while self._running and self._socket:
            try:
                data, addr = await loop.sock_recvfrom(
                    self._socket,
                    self.config.receive_buffer_size,
                )

                current_addr = addr

                if not data:
                    continue

                msg_type = data[0:1]
                current_msg_type = msg_type

                if msg_type == OOB_PROBE:
                    # Handle incoming probe
                    self._probes_received += 1
                    await self._handle_probe(data, addr)

                elif msg_type in (OOB_ACK, OOB_NACK):
                    # Handle response to our probe
                    self._handle_response(msg_type, addr)

            except asyncio.CancelledError:
                await self._log_error("Receive loop cancelled")
                break
            except Exception as receive_error:
                msg_type_hex = current_msg_type.hex() if current_msg_type else "unknown"
                addr_str = (
                    f"{current_addr[0]}:{current_addr[1]}"
                    if current_addr
                    else "unknown"
                )
                await self._log_error(
                    f"Receive loop error: {receive_error}, "
                    f"socket={self.host}:{self.port}, "
                    f"remote_addr={addr_str}, "
                    f"msg_type=0x{msg_type_hex}"
                )
                current_addr = None
                current_msg_type = None
                continue

    async def _handle_probe(self, data: bytes, addr: tuple[str, int]) -> None:
        """Handle incoming probe request."""
        if not self._socket:
            return

        # Determine response type
        if (
            self.config.send_nack_when_overloaded
            and self._is_overloaded
            and self._is_overloaded()
        ):
            response = OOB_NACK
            self._nacks_sent += 1
        else:
            response = OOB_ACK
            self._acks_sent += 1

        # Extract reply address from probe if present
        try:
            if len(data) > 1:
                reply_addr_str = data[1:].decode()
                if ":" in reply_addr_str:
                    host, port = reply_addr_str.rsplit(":", 1)
                    reply_addr = (host, int(port))
                else:
                    reply_addr = addr
            else:
                reply_addr = addr
        except Exception:
            reply_addr = addr

        # Send response
        try:
            await asyncio.get_event_loop().sock_sendto(
                self._socket,
                response,
                reply_addr,
            )
        except Exception as send_error:
            # The prober sees a timeout: count and log the real cause.
            self._reply_send_failures += 1
            await self._log_error(
                f"OOB health reply to {reply_addr[0]}:{reply_addr[1]} failed: {send_error!r}"
            )

    def _handle_response(self, msg_type: bytes, addr: tuple[str, int]) -> None:
        """Handle response to our probe."""
        future = self._pending_probes.get(addr)
        if future and not future.done():
            future.set_result(msg_type)

    def _check_rate_limit(self, target: tuple[str, int]) -> bool:
        """Check if we can send a probe (rate limiting)."""
        now = _DEFAULT_CLOCK.monotonic()

        # Per-target cooldown
        last_probe = self._last_probe_time.get(target, 0)
        if now - last_probe < self.config.per_target_cooldown_seconds:
            return False

        # Global rate limit
        if now - self._global_probe_window_start > 1.0:
            self._global_probe_count = 0
            self._global_probe_window_start = now

        if self._global_probe_count >= self.config.max_probes_per_second:
            return False

        self._global_probe_count += 1
        return True

    def cleanup_stale_rate_limits(self, max_age_seconds: float = 60.0) -> int:
        """
        Clean up stale rate limit entries.

        Returns:
            Number of entries removed
        """
        now = _DEFAULT_CLOCK.monotonic()
        stale = [
            target
            for target, last_time in self._last_probe_time.items()
            if now - last_time > max_age_seconds
        ]

        for target in stale:
            del self._last_probe_time[target]

        return len(stale)

    def get_stats(self) -> dict[str, int | float]:
        """Get channel statistics."""
        return {
            "port": self.port,
            "running": self._running,
            "probes_sent": self._probes_sent,
            "probes_received": self._probes_received,
            "acks_sent": self._acks_sent,
            "nacks_sent": self._nacks_sent,
            "timeouts": self._timeouts,
            "reply_send_failures": self._reply_send_failures,
            "pending_probes": len(self._pending_probes),
            "rate_limit_entries": len(self._last_probe_time),
        }


def get_oob_port_for_swim_port(swim_port: int, offset: int = 100) -> int:
    """
    Get the OOB port for a given SWIM UDP port.

    Args:
        swim_port: The main SWIM UDP port
        offset: Port offset for OOB channel

    Returns:
        The OOB channel port number
    """
    return swim_port + offset

_REHOMED = (
    OOBHealthChannelConfig,
    OOBProbeResult,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
