"""
Job Forwarding Tracker - Cross-gate job forwarding for gates.

This class encapsulates the logic for forwarding job-related messages
(progress updates, final results) to peer gates when a gate receives
messages for jobs it doesn't own.

Key responsibilities:
- Track known peer gates for forwarding
- Forward job progress to appropriate peer gates
- Forward final results to appropriate peer gates
- Track forwarding statistics and failures
"""

import time
from dataclasses import dataclass, field
from typing import Protocol, Callable, Awaitable


@dataclass(slots=True)
class GatePeerInfo:
    """Information about a peer gate for forwarding."""

    gate_id: str
    tcp_host: str
    tcp_port: int
    last_seen: float = 0.0
    forward_failures: int = 0
    forward_successes: int = 0


@dataclass(slots=True)
class ForwardingResult:
    """Result of a forwarding attempt."""

    forwarded: bool
    target_gate_id: str | None = None
    error: str | None = None


class SendTcpProtocol(Protocol):
    """Protocol for TCP send function."""

    async def __call__(
        self,
        addr: tuple[str, int],
        endpoint: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> bytes: ...


class JobForwardingTracker:
    """
    Tracks peer gates and handles cross-gate job forwarding.

    When a gate receives a job update (progress or final result) for a job
    it doesn't own, it uses this tracker to forward the message to peer
    gates that may own the job.

    Example usage:
        tracker = JobForwardingTracker()
        tracker.register_peer("gate-2", "10.0.0.2", 8080)

        # Forward a result
        result = await tracker.forward_result(
            job_id="job-123",
            data=result.dump(),
            send_tcp=gate_server.send_tcp,
        )
        if result.forwarded:
            print(f"Forwarded to {result.target_gate_id}")
    """

    def __init__(
        self,
        local_gate_id: str = "",
        forward_timeout: float = 3.0,
        max_forward_attempts: int = 3,
    ):
        """
        Initialize JobForwardingTracker.

        Args:
            local_gate_id: ID of the local gate (to avoid forwarding to self).
            forward_timeout: Timeout for forwarding TCP calls.
            max_forward_attempts: Maximum peers to try before giving up.
        """
        self._local_gate_id = local_gate_id
        self._forward_timeout = forward_timeout
        self._max_forward_attempts = max_forward_attempts

        # Known peer gates: gate_id -> GatePeerInfo
        self._peers: dict[str, GatePeerInfo] = {}

        # Forwarding statistics
        self._total_forwards: int = 0
        self._successful_forwards: int = 0
        self._failed_forwards: int = 0

    # =========================================================================
    # Peer Management
    # =========================================================================

    def set_local_gate_id(self, gate_id: str) -> None:
        """Set the local gate ID (to avoid forwarding to self)."""
        self._local_gate_id = gate_id

    def register_peer(
        self,
        gate_id: str,
        tcp_host: str,
        tcp_port: int,
    ) -> None:
        """
        Register or update a peer gate for forwarding.

        Args:
            gate_id: Unique identifier of the peer gate.
            tcp_host: TCP host address of the peer.
            tcp_port: TCP port of the peer.
        """
        if gate_id == self._local_gate_id:
            return  # Don't register self

        existing = self._peers.get(gate_id)
        if existing:
            existing.tcp_host = tcp_host
            existing.tcp_port = tcp_port
            existing.last_seen = time.monotonic()
        else:
            self._peers[gate_id] = GatePeerInfo(
                gate_id=gate_id,
                tcp_host=tcp_host,
                tcp_port=tcp_port,
                last_seen=time.monotonic(),
            )

    def unregister_peer(self, gate_id: str) -> None:
        """Remove a peer gate from the forwarding list."""
        self._peers.pop(gate_id, None)

    def get_peer(self, gate_id: str) -> GatePeerInfo | None:
        """Get peer info by gate ID."""
        return self._peers.get(gate_id)

    def get_all_peers(self) -> list[GatePeerInfo]:
        """Get all registered peers."""
        return list(self._peers.values())

    def peer_count(self) -> int:
        """Get the number of registered peers."""
        return len(self._peers)

    def update_peer_from_heartbeat(
        self,
        gate_id: str,
        tcp_host: str,
        tcp_port: int,
    ) -> None:
        """
        Update peer info from a heartbeat message.

        This is called when receiving gate heartbeats to keep
        peer information up to date.
        """
        self.register_peer(gate_id, tcp_host, tcp_port)

    # =========================================================================
    # Forwarding
    # =========================================================================

    async def forward_progress(
        self,
        job_id: str,
        data: bytes,
        send_tcp: SendTcpProtocol,
    ) -> ForwardingResult:
        """
        Forward job progress to peer gates.

        Tries peers in order until one succeeds or max attempts reached.

        Args:
            job_id: The job ID being forwarded.
            data: Serialized JobProgress message.
            send_tcp: TCP send function to use.

        Returns:
            ForwardingResult indicating success/failure.
        """
        return await self._forward_message(
            job_id=job_id,
            endpoint="job_progress",
            data=data,
            send_tcp=send_tcp,
            timeout=2.0,  # Progress updates can be shorter timeout
        )

    async def forward_result(
        self,
        job_id: str,
        data: bytes,
        send_tcp: SendTcpProtocol,
    ) -> ForwardingResult:
        """
        Forward job final result to peer gates.

        Tries peers in order until one succeeds or max attempts reached.

        Args:
            job_id: The job ID being forwarded.
            data: Serialized JobFinalResult message.
            send_tcp: TCP send function to use.

        Returns:
            ForwardingResult indicating success/failure.
        """
        return await self._forward_message(
            job_id=job_id,
            endpoint="job_final_result",
            data=data,
            send_tcp=send_tcp,
            timeout=self._forward_timeout,
        )

    async def _forward_message(
        self,
        job_id: str,
        endpoint: str,
        data: bytes,
        send_tcp: SendTcpProtocol,
        timeout: float,
    ) -> ForwardingResult:
        """
        Internal method to forward a message to peer gates.

        Tries peers in order, stopping after first success.
        """
        self._total_forwards += 1

        if not self._peers:
            self._failed_forwards += 1
            return ForwardingResult(
                forwarded=False,
                error="No peer gates registered",
            )

        attempts = 0
        last_error: str | None = None

        for gate_id, peer in list(self._peers.items()):
            if attempts >= self._max_forward_attempts:
                break

            try:
                addr = (peer.tcp_host, peer.tcp_port)
                await send_tcp(addr, endpoint, data, timeout)

                # Success
                peer.forward_successes += 1
                peer.last_seen = time.monotonic()
                self._successful_forwards += 1

                return ForwardingResult(
                    forwarded=True,
                    target_gate_id=gate_id,
                )

            except Exception as exception:
                peer.forward_failures += 1
                last_error = str(exception)
                attempts += 1
                continue

        # All attempts failed
        self._failed_forwards += 1
        return ForwardingResult(
            forwarded=False,
            error=last_error or "All forward attempts failed",
        )

    # =========================================================================
    # Statistics
    # =========================================================================

    def get_stats(self) -> dict:
        """Get forwarding statistics."""
        return {
            "peer_count": len(self._peers),
            "total_forwards": self._total_forwards,
            "successful_forwards": self._successful_forwards,
            "failed_forwards": self._failed_forwards,
            "success_rate": (
                self._successful_forwards / self._total_forwards
                if self._total_forwards > 0
                else 0.0
            ),
            "peers": {
                gate_id: {
                    "tcp_host": peer.tcp_host,
                    "tcp_port": peer.tcp_port,
                    "forward_successes": peer.forward_successes,
                    "forward_failures": peer.forward_failures,
                    "last_seen": peer.last_seen,
                }
                for gate_id, peer in self._peers.items()
            },
        }

    def reset_stats(self) -> None:
        """Reset forwarding statistics."""
        self._total_forwards = 0
        self._successful_forwards = 0
        self._failed_forwards = 0

        for peer in self._peers.values():
            peer.forward_successes = 0
            peer.forward_failures = 0

    # =========================================================================
    # Cleanup
    # =========================================================================

    def cleanup_stale_peers(self, max_age_seconds: float = 300.0) -> list[str]:
        """
        Remove peers not seen within max_age_seconds.

        Returns list of removed gate IDs.
        """
        now = time.monotonic()
        to_remove: list[str] = []

        for gate_id, peer in list(self._peers.items()):
            if peer.last_seen > 0 and (now - peer.last_seen) > max_age_seconds:
                to_remove.append(gate_id)

        for gate_id in to_remove:
            self._peers.pop(gate_id, None)

        return to_remove
