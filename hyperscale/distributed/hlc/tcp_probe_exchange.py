from __future__ import annotations

from typing import Any, Awaitable, Callable

from hyperscale.distributed.hlc.clock_offset_probe_error import ClockOffsetProbeError

# The TCP action peers serve clock offset probes under.
CLOCK_OFFSET_PROBE_ACTION = "clock_offset_probe"

PeerAddress = tuple[str, int]


def tcp_probe_exchange(
    send_tcp: Callable[..., Awaitable[tuple[Any, Any]]],
) -> Callable[[PeerAddress, bytes], Awaitable[bytes]]:
    """A probe exchange over a server's ``send_tcp``, which reports
    failures as values: they become ``ClockOffsetProbeError``.

    No timeout is passed -- the server's request timeout applies. A probe
    timeout would close the transport the peer shares with every other
    request, and a late reply is still a sound (wider) bound.
    """

    async def exchange(address: PeerAddress, request: bytes) -> bytes:
        reply, _ = await send_tcp(address, CLOCK_OFFSET_PROBE_ACTION, request)
        if isinstance(reply, Exception):
            raise ClockOffsetProbeError(f"no reply: {reply!r}") from reply
        if not isinstance(reply, bytes) or not reply:
            raise ClockOffsetProbeError(f"no reply: {reply!r}")
        return reply

    return exchange
