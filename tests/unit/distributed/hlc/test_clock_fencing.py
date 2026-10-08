"""
AD-39 majority clock fencing: offset measurement and the fence verdict.

* Bounds are sound: over seeded random skews, link delays, reply
  processing times and wall-clock steps during the exchange, the true
  offset always lies inside the measured bound.
* Quorum: a node fences only when a quorum of its configured cluster
  (itself counted as agreeing) measured it certainly beyond the
  threshold. One fast peer never fences the healthy majority -- each
  healthy node sees only that one peer beyond -- while the fast peer,
  seen beyond by everyone, fences itself. In a two-node cluster neither
  can fence the other.
* A measurement whose bound straddles the threshold (a slow link) never
  counts: only certainty fences.
* The node's own HLC leading its physical clock by more than the offset
  bound fences it (its clock stepped back after minting); a lead up to
  the bound -- merging an in-bound fast peer -- does not.
* Expired measurements stop counting, so a fenced node unfences once its
  clock agrees again; departed peers are forgotten.
* The prober reports each transition exactly once, and a failed probe is
  reported and skipped, never raised.
"""

import random

import pytest

from hyperscale.distributed.hlc import (
    ClockOffsetBounds,
    ClockOffsetMonitor,
    ClockOffsetProbeError,
    ClockOffsetProber,
    HLCTimestamp,
    HybridLogicalClock,
)
from hyperscale.distributed.hlc.clock_offset_monitor import FENCE_FRACTION_OF_MAX_OFFSET
from hyperscale.distributed.hlc.models import ClockOffsetProbe, ClockOffsetProbeReply
from hyperscale.logging.hyperscale_logging_models import (
    ClockFenced,
    ClockOffsetProbeFailed,
    ClockUnfenced,
)
from tests.unit.distributed.hlc.settable_clock import SettableClock

MAX_OFFSET_MS = 500
THRESHOLD_MS = int(MAX_OFFSET_MS * FENCE_FRACTION_OF_MAX_OFFSET)
EPOCH_MS = 1_790_000_000_000
SAMPLE_TTL_SECONDS = 3.0
PROBE_INTERVAL_SECONDS = 1.0
BOUNDS_FUZZ_SEEDS = range(50)
BOUNDS_FUZZ_EXCHANGES = 2_000


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list = []

    async def log(self, entry) -> None:
        self.entries.append(entry)

    def of_type(self, entry_type: type) -> list:
        return [entry for entry in self.entries if isinstance(entry, entry_type)]


def make_monitor(
    cluster_size: int, physical: SettableClock | None = None
) -> tuple[ClockOffsetMonitor, HybridLogicalClock, SettableClock]:
    physical = physical if physical is not None else SettableClock(EPOCH_MS)
    hlc = HybridLogicalClock(node_id=1, clock=physical, max_offset_ms=MAX_OFFSET_MS)
    monitor = ClockOffsetMonitor(
        hlc=hlc,
        cluster_size=lambda: cluster_size,
        sample_ttl_seconds=SAMPLE_TTL_SECONDS,
        clock=physical,
    )
    return monitor, hlc, physical


def exact_bounds(offset_ms: int, measured_at: float) -> ClockOffsetBounds:
    return ClockOffsetBounds(lower_ms=offset_ms, upper_ms=offset_ms, measured_at=measured_at)


# ------------------------------------------------------------------ bounds


@pytest.mark.parametrize("seed", BOUNDS_FUZZ_SEEDS)
def test_measured_bounds_always_contain_the_true_offset(seed: int) -> None:
    random_source = random.Random(seed)
    for _ in range(BOUNDS_FUZZ_EXCHANGES):
        true_time_ms = EPOCH_MS + random_source.uniform(0, 10_000_000)
        self_skew_ms = random_source.uniform(-5_000, 5_000)
        peer_skew_ms = random_source.uniform(-5_000, 5_000)
        outbound_delay_ms = random_source.expovariate(1 / 20)
        processing_ms = random_source.expovariate(1 / 5)
        inbound_delay_ms = random_source.expovariate(1 / 20)
        # This node's wall clock may be stepped mid-exchange; the round
        # trip is timed on the monotonic clock, which is not.
        sent_physical_ms = int(true_time_ms + self_skew_ms)
        peer_physical_ms = int(true_time_ms + outbound_delay_ms + processing_ms + peer_skew_ms)
        round_trip_seconds = (outbound_delay_ms + processing_ms + inbound_delay_ms) / 1000
        round_trip_ms = int(-(-round_trip_seconds * 1000 // 1))

        bounds = ClockOffsetBounds.from_round_trip(
            sent_physical_ms=sent_physical_ms,
            round_trip_ms=round_trip_ms,
            peer_physical_ms=peer_physical_ms,
            measured_at=0.0,
        )

        true_offset_ms = peer_skew_ms - self_skew_ms
        assert bounds.lower_ms <= true_offset_ms <= bounds.upper_ms, (seed, bounds, true_offset_ms)


def test_a_bound_straddling_the_threshold_is_never_certain() -> None:
    straddling = ClockOffsetBounds(lower_ms=THRESHOLD_MS - 50, upper_ms=THRESHOLD_MS + 50, measured_at=0.0)
    assert not straddling.certainly_beyond(THRESHOLD_MS)
    assert ClockOffsetBounds(THRESHOLD_MS + 1, THRESHOLD_MS + 90, 0.0).certainly_beyond(THRESHOLD_MS)
    assert ClockOffsetBounds(-THRESHOLD_MS - 90, -THRESHOLD_MS - 1, 0.0).certainly_beyond(THRESHOLD_MS)


# ----------------------------------------------------------------- monitor


def test_a_quorum_of_peers_beyond_the_threshold_fences() -> None:
    monitor, _hlc, physical = make_monitor(cluster_size=3)
    now = physical.monotonic()
    monitor.record("peer-a", exact_bounds(-(THRESHOLD_MS + 1), now))
    assert not monitor.refresh().fenced

    monitor.record("peer-b", exact_bounds(-(THRESHOLD_MS + 1), now))
    verdict = monitor.refresh()
    assert (verdict.fenced, verdict.peers_beyond, verdict.quorum) == (True, 2, 2)


def test_one_fast_peer_fences_only_itself() -> None:
    """Three nodes, one 2s fast. Each healthy node measures the fast one
    beyond and the other healthy one in agreement; the fast node
    measures both beyond."""
    fast_by_ms = 2_000
    healthy_view, _, healthy_physical = make_monitor(cluster_size=3)
    now = healthy_physical.monotonic()
    healthy_view.record("fast", exact_bounds(fast_by_ms, now))
    healthy_view.record("other-healthy", exact_bounds(0, now))

    fast_view, _, fast_physical = make_monitor(cluster_size=3)
    fast_now = fast_physical.monotonic()
    fast_view.record("healthy-a", exact_bounds(-fast_by_ms, fast_now))
    fast_view.record("healthy-b", exact_bounds(-fast_by_ms, fast_now))

    assert not healthy_view.refresh().fenced
    assert fast_view.refresh().fenced


def test_in_a_two_node_cluster_neither_clock_can_fence_the_other() -> None:
    monitor, _hlc, physical = make_monitor(cluster_size=2)
    monitor.record("only-peer", exact_bounds(10 * MAX_OFFSET_MS, physical.monotonic()))
    assert not monitor.refresh().fenced


def test_hlc_lead_beyond_the_bound_fences_and_a_lead_within_it_does_not() -> None:
    monitor, hlc, physical = make_monitor(cluster_size=1)
    hlc.receive(HLCTimestamp(wall_ms=EPOCH_MS + MAX_OFFSET_MS, logical=0, node_id=2))
    assert not monitor.refresh().fenced  # a merged in-bound peer timestamp

    physical.unix_ms -= 1  # this node's clock steps back
    verdict = monitor.refresh()
    assert verdict.fenced and verdict.hlc_lead_ms == MAX_OFFSET_MS + 1

    physical.unix_ms += 2  # physical time catches up
    assert not monitor.refresh().fenced


def test_expired_measurements_stop_counting() -> None:
    monitor, _hlc, physical = make_monitor(cluster_size=3)
    measured_at = physical.monotonic()
    for peer_id in ("peer-a", "peer-b"):
        monitor.record(peer_id, exact_bounds(THRESHOLD_MS + 1, measured_at))
    assert monitor.refresh().fenced

    physical.unix_ms += int(SAMPLE_TTL_SECONDS * 1000)
    assert monitor.refresh().fenced  # exactly at the TTL still counts
    physical.unix_ms += 1
    verdict = monitor.refresh()
    assert (verdict.fenced, verdict.peers_measured) == (False, 0)


def test_a_monitor_never_refreshed_is_not_fenced() -> None:
    monitor, _hlc, _physical = make_monitor(cluster_size=3)
    assert monitor.verdict is None and not monitor.is_fenced


# ------------------------------------------------------------------ prober


class PeerClocks:
    """Answers probes from peers whose physical clocks sit at fixed
    offsets from the prober's, failing for peers marked down."""

    def __init__(self, prober_physical: SettableClock, offsets_ms: dict[tuple[str, int], int]) -> None:
        self.prober_physical = prober_physical
        self.offsets_ms = offsets_ms
        self.down: set[tuple[str, int]] = set()
        self.requests: list[ClockOffsetProbe] = []

    async def exchange(self, address: tuple[str, int], request: bytes) -> bytes:
        self.requests.append(ClockOffsetProbe.load(request))
        if address in self.down:
            raise ClockOffsetProbeError("peer unreachable")
        return ClockOffsetProbeReply(
            responder_id=str(address),
            responder_physical_ms=self.prober_physical.unix_ms + self.offsets_ms[address],
        ).dump()


def make_prober(
    peers: dict[str, tuple[str, int]], offsets_ms: dict[tuple[str, int], int]
) -> tuple[ClockOffsetProber, ClockOffsetMonitor, PeerClocks, RecordingLogger, list]:
    monitor, hlc, physical = make_monitor(cluster_size=len(peers) + 1)
    peer_clocks = PeerClocks(physical, offsets_ms)
    logger = RecordingLogger()
    transitions: list = []

    async def on_fence_change(verdict) -> None:
        transitions.append(verdict.fenced)

    prober = ClockOffsetProber(
        node_id="self",
        hlc=hlc,
        monitor=monitor,
        peers=lambda: peers,
        exchange=peer_clocks.exchange,
        clock=physical,
        probe_interval_seconds=PROBE_INTERVAL_SECONDS,
        logger=logger,
        on_fence_change=on_fence_change,
    )
    return prober, monitor, peer_clocks, logger, transitions


@pytest.mark.asyncio
async def test_prober_fences_and_unfences_once_per_transition() -> None:
    peers = {"peer-a": ("10.0.0.1", 9000), "peer-b": ("10.0.0.2", 9000)}
    offsets = {address: -2_000 for address in peers.values()}  # this node runs 2s fast
    prober, _monitor, peer_clocks, logger, transitions = make_prober(peers, offsets)

    for _ in range(3):
        await prober.probe_round()
    assert transitions == [True]
    assert [entry.peers_beyond for entry in logger.of_type(ClockFenced)] == [2]
    assert {request.prober_id for request in peer_clocks.requests} == {"self"}

    for address in peers.values():
        offsets[address] = 0
    for _ in range(3):
        await prober.probe_round()
    assert transitions == [True, False]
    assert len(logger.of_type(ClockUnfenced)) == 1


@pytest.mark.asyncio
async def test_a_failed_probe_is_reported_and_skipped() -> None:
    peers = {"peer-a": ("10.0.0.1", 9000), "peer-b": ("10.0.0.2", 9000)}
    prober, monitor, peer_clocks, logger, transitions = make_prober(
        peers, {address: -2_000 for address in peers.values()}
    )
    peer_clocks.down.add(peers["peer-b"])

    verdict = await prober.probe_round()

    assert (verdict.fenced, verdict.peers_measured) == (False, 1)
    assert [entry.peer_id for entry in logger.of_type(ClockOffsetProbeFailed)] == ["peer-b"]
    assert transitions == []
    assert monitor.measured_peers == {"peer-a"}


@pytest.mark.asyncio
async def test_departed_peers_are_forgotten() -> None:
    peers = {"peer-a": ("10.0.0.1", 9000), "peer-b": ("10.0.0.2", 9000)}
    prober, monitor, _peer_clocks, _logger, _transitions = make_prober(
        peers, {address: 0 for address in peers.values()}
    )
    await prober.probe_round()
    assert monitor.measured_peers == {"peer-a", "peer-b"}

    del peers["peer-b"]
    await prober.probe_round()
    assert monitor.measured_peers == {"peer-a"}


@pytest.mark.asyncio
async def test_a_reply_that_is_not_a_probe_reply_is_a_failed_probe() -> None:
    peers = {"peer-a": ("10.0.0.1", 9000)}
    prober, monitor, peer_clocks, logger, _transitions = make_prober(peers, {peers["peer-a"]: 0})

    async def wrong_reply(address, request) -> bytes:
        return ClockOffsetProbe(prober_id="not-a-reply").dump()

    prober._exchange = wrong_reply
    await prober.probe_round()

    assert monitor.measured_peers == frozenset()
    assert len(logger.of_type(ClockOffsetProbeFailed)) == 1
