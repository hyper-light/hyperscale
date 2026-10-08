from __future__ import annotations

import asyncio
import math
from typing import TYPE_CHECKING, Awaitable, Callable, Mapping

from hyperscale.distributed.hlc.clock_fence_verdict import ClockFenceVerdict
from hyperscale.distributed.hlc.clock_offset_bounds import ClockOffsetBounds
from hyperscale.distributed.hlc.clock_offset_monitor import ClockOffsetMonitor
from hyperscale.distributed.hlc.clock_offset_probe_error import ClockOffsetProbeError
from hyperscale.distributed.hlc.hybrid_logical_clock import HybridLogicalClock
from hyperscale.distributed.hlc.models import ClockOffsetProbe, ClockOffsetProbeReply
from hyperscale.distributed.runtime import Clock
from hyperscale.logging.hyperscale_logging_models import (
    ClockFenced,
    ClockOffsetProbeFailed,
    ClockUnfenced,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger

PeerAddress = tuple[str, int]
ProbeExchange = Callable[[PeerAddress, bytes], Awaitable[bytes]]


class ClockOffsetProber:
    """Measures this node's clock offset to each peer, every
    ``probe_interval_seconds``, and keeps the fence verdict current
    (AD-39).

    Each probe is one round trip: the bound it yields is exactly as wide
    as the round trip, so a slow or congested link widens the bound
    instead of skewing the estimate. ``exchange`` sends the request and
    returns the reply bytes, raising ``ClockOffsetProbeError`` when there
    is none. ``on_fence_change`` runs on every fence transition.
    """

    __slots__ = (
        "_node_id",
        "_hlc",
        "_monitor",
        "_peers",
        "_exchange",
        "_clock",
        "_probe_interval_seconds",
        "_logger",
        "_on_fence_change",
        "_running",
    )

    def __init__(
        self,
        node_id: str,
        hlc: HybridLogicalClock,
        monitor: ClockOffsetMonitor,
        peers: Callable[[], Mapping[str, PeerAddress]],
        exchange: ProbeExchange,
        clock: Clock,
        probe_interval_seconds: float,
        logger: "Logger",
        on_fence_change: Callable[[ClockFenceVerdict], Awaitable[None]],
    ) -> None:
        if probe_interval_seconds <= 0.0:
            raise ValueError(f"probe_interval_seconds must be positive, got {probe_interval_seconds}")
        self._node_id = node_id
        self._hlc = hlc
        self._monitor = monitor
        self._peers = peers
        self._exchange = exchange
        self._clock = clock
        self._probe_interval_seconds = probe_interval_seconds
        self._logger = logger
        self._on_fence_change = on_fence_change
        self._running = False

    async def run(self) -> None:
        """Probe every interval until ``stop``."""
        self._running = True
        while self._running:
            await self.probe_round()
            await self._clock.sleep(self._probe_interval_seconds)

    def stop(self) -> None:
        self._running = False

    async def probe_round(self) -> ClockFenceVerdict:
        """Probe every current peer once, then re-evaluate the fence."""
        peers = dict(self._peers())
        self._forget_departed_peers(peers)
        await asyncio.gather(*(self._probe(peer_id, address) for peer_id, address in peers.items()))

        was_fenced = self._monitor.is_fenced
        verdict = self._monitor.refresh()
        if verdict.fenced != was_fenced:
            await self._report_transition(verdict)
            await self._on_fence_change(verdict)
        return verdict

    def _forget_departed_peers(self, peers: dict[str, PeerAddress]) -> None:
        """Drop the measurements of peers no longer in the membership."""
        for departed_peer in self._monitor.measured_peers - peers.keys():
            self._monitor.forget(departed_peer)

    async def _probe(self, peer_id: str, address: PeerAddress) -> None:
        request = ClockOffsetProbe(prober_id=self._node_id).dump()
        sent_physical_ms = self._hlc.physical_ms()
        sent_at = self._clock.monotonic()
        try:
            reply = self._decode(await self._exchange(address, request))
        except ClockOffsetProbeError as probe_error:
            await self._logger.log(
                ClockOffsetProbeFailed(
                    message=f"Clock offset probe to {peer_id} at {address} failed: {probe_error}",
                    node_id=self._node_id,
                    peer_id=peer_id,
                    error_type=type(probe_error.__cause__ or probe_error).__name__,
                )
            )
            return
        received_at = self._clock.monotonic()
        self._monitor.record(
            peer_id,
            ClockOffsetBounds.from_round_trip(
                sent_physical_ms=sent_physical_ms,
                round_trip_ms=math.ceil((received_at - sent_at) * 1000),
                peer_physical_ms=reply.responder_physical_ms,
                measured_at=received_at,
            ),
        )

    @staticmethod
    def _decode(reply: bytes) -> ClockOffsetProbeReply:
        try:
            decoded = ClockOffsetProbeReply.load(reply)
        except Exception as decode_error:
            raise ClockOffsetProbeError(f"undecodable reply: {decode_error!r}") from decode_error
        if not isinstance(decoded, ClockOffsetProbeReply):
            raise ClockOffsetProbeError(f"reply is a {type(decoded).__name__}, not a probe reply")
        return decoded

    async def _report_transition(self, verdict: ClockFenceVerdict) -> None:
        details = (
            f"{verdict.peers_beyond}/{verdict.peers_measured} measured peers beyond "
            f"{verdict.threshold_ms}ms (quorum {verdict.quorum}), HLC leads physical by {verdict.hlc_lead_ms}ms"
        )
        fields = {
            "node_id": self._node_id,
            "peers_beyond": verdict.peers_beyond,
            "peers_measured": verdict.peers_measured,
            "quorum": verdict.quorum,
            "hlc_lead_ms": verdict.hlc_lead_ms,
            "threshold_ms": verdict.threshold_ms,
        }
        if verdict.fenced:
            await self._logger.log(
                ClockFenced(message=f"Clock fenced: refusing leadership and new work ({details})", **fields)
            )
        else:
            await self._logger.log(ClockUnfenced(message=f"Clock unfenced ({details})", **fields))
