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
"""

import asyncio
import socket
import time
from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed.swim.core.protocols import LoggerProtocol


# Message format: single byte type + payload
OOB_PROBE = b"\x01"  # Health probe request
OOB_ACK = b"\x02"  # Health probe acknowledgment
OOB_NACK = b"\x03"  # Health probe negative acknowledgment (overloaded)

# Maximum OOB message size (minimal for fast processing)
MAX_OOB_MESSAGE_SIZE = 64

# Rate limiting for OOB channel
OOB_MAX_PROBES_PER_SECOND = 100
OOB_PROBE_COOLDOWN = 0.01  # 10ms between probes to same target


@dataclass(slots=True)
class OOBHealthChannelConfig:
    """Configuration for out-of-band health channel."""

    # Port offset from main UDP port (e.g., if main is 8000, OOB is 8000 + offset)
    port_offset: int = 100

    # Timeout for OOB probes (shorter than regular probes)
    probe_timeout_seconds: float = 0.5

    # Maximum probes per second (global rate limit)
    max_probes_per_second: int = OOB_MAX_PROBES_PER_SECOND

    # Cooldown between probes to same target
    per_target_cooldown_seconds: float = OOB_PROBE_COOLDOWN

    # Buffer size for receiving
    receive_buffer_size: int = MAX_OOB_MESSAGE_SIZE

    # Enable NACK responses when overloaded
    send_nack_when_overloaded: bool = True


@dataclass(slots=True)
class OOBProbeResult:
    """Result of an out-of-band probe."""

    target: tuple[str, int]
    success: bool
    is_overloaded: bool  # True if received NACK
    latency_ms: float
    error: str | None = None


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
    _global_probe_window_start: float = field(default_factory=time.monotonic)

    # Callback for when we receive a probe (to generate response)
    _is_overloaded: Callable[[], bool] | None = None

    # Statistics
    _probes_sent: int = 0
    _probes_received: int = 0
    _acks_sent: int = 0
    _nacks_sent: int = 0
    _timeouts: int = 0

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
            from hyperscale.logging.hyperscale_logging_models import ServerError

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
        self._receive_task = asyncio.create_task(self._receive_loop())

    async def stop(self) -> None:
        """Stop the OOB health channel."""
        self._running = False

        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
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

        start_time = time.monotonic()

        try:
            # Send probe
            message = OOB_PROBE + f"{self.host}:{self.port}".encode()
            await asyncio.get_event_loop().sock_sendto(
                self._socket,
                message,
                target,
            )
            self._probes_sent += 1
            self._last_probe_time[target] = time.monotonic()

            # Wait for response
            try:
                response = await asyncio.wait_for(
                    future,
                    timeout=self.config.probe_timeout_seconds,
                )

                latency = (time.monotonic() - start_time) * 1000
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
                    latency_ms=(time.monotonic() - start_time) * 1000,
                    error="Timeout",
                )

            except asyncio.CancelledError:
                # Probe was cancelled (e.g., during shutdown)
                # Return graceful failure instead of propagating
                return OOBProbeResult(
                    target=target,
                    success=False,
                    is_overloaded=False,
                    latency_ms=(time.monotonic() - start_time) * 1000,
                    error="Cancelled",
                )

        except asyncio.CancelledError:
            # Cancelled during send - graceful failure
            return OOBProbeResult(
                target=target,
                success=False,
                is_overloaded=False,
                latency_ms=(time.monotonic() - start_time) * 1000,
                error="Cancelled",
            )

        except Exception as e:
            return OOBProbeResult(
                target=target,
                success=False,
                is_overloaded=False,
                latency_ms=(time.monotonic() - start_time) * 1000,
                error=str(e),
            )

        finally:
            self._pending_probes.pop(target, None)

    async def _receive_loop(self) -> None:
        """Receive loop for OOB messages."""
        loop = asyncio.get_event_loop()

        while self._running and self._socket:
            try:
                data, addr = await loop.sock_recvfrom(
                    self._socket,
                    self.config.receive_buffer_size,
                )

                if not data:
                    continue

                msg_type = data[0:1]

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
                await self._log_error(f"Receive loop error: {receive_error}")
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
                    host, port = reply_addr_str.split(":", 1)
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
        except Exception:
            pass  # Best effort

    def _handle_response(self, msg_type: bytes, addr: tuple[str, int]) -> None:
        """Handle response to our probe."""
        future = self._pending_probes.get(addr)
        if future and not future.done():
            future.set_result(msg_type)

    def _check_rate_limit(self, target: tuple[str, int]) -> bool:
        """Check if we can send a probe (rate limiting)."""
        now = time.monotonic()

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
        now = time.monotonic()
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
