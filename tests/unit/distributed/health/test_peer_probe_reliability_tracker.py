from hyperscale.distributed.swim.detection.peer_probe_reliability_tracker import (
    PeerProbeReliabilityConfig,
    PeerProbeReliabilityTracker,
)


def test_had_success_since_ignores_success_before_epoch() -> None:
    """A pre-burst success must not refute a later burst-failure decision."""
    tracker = PeerProbeReliabilityTracker(PeerProbeReliabilityConfig())
    peer = ("127.0.0.1", 9001)

    tracker.record_probe_outcome(peer, success=True, now=10.0)

    assert tracker.had_success_since(peer, since=11.0, now=12.0) is False


def test_had_success_since_reports_latest_success_after_epoch() -> None:
    """A latest post-epoch success is positive liveness evidence."""
    tracker = PeerProbeReliabilityTracker(PeerProbeReliabilityConfig())
    peer = ("127.0.0.1", 9001)

    tracker.record_probe_outcome(peer, success=False, now=10.0)
    tracker.record_probe_outcome(peer, success=True, now=11.0)

    assert tracker.had_success_since(peer, since=10.5, now=12.0) is True


def test_had_success_since_latest_failure_overrides_prior_success() -> None:
    """A newer failure after the epoch means the peer is currently silent."""
    tracker = PeerProbeReliabilityTracker(PeerProbeReliabilityConfig())
    peer = ("127.0.0.1", 9001)

    tracker.record_probe_outcome(peer, success=True, now=10.0)
    tracker.record_probe_outcome(peer, success=False, now=11.0)

    assert tracker.had_success_since(peer, since=9.0, now=12.0) is False
