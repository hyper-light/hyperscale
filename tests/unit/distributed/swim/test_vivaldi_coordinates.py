"""
Vivaldi coordinates (AD-35) as routing and SWIM use them.

A coordinate's error is its moving-average absolute misprediction in
seconds, but it was floored at 0.05 -- a relative-error constant read as
seconds -- so every RTT upper bound sat at least 200ms above the RTT
however well two nodes had measured each other; "converged" meant an
error under half a second. Coordinates arriving on the wire decoded with
defaults for missing fields (an absent vector became an empty one) and a
malformed one was swallowed without a trace; a vector of the wrong
dimension silently truncated distances. Unconfirmed peers' minutes-long
passive timeouts were multiplied by up to 10x for sub-second RTTs.

* converged coordinates bound the RTT tightly, and convergence means the
  samples and error that earn full quality;
* a coordinate decodes only whole, and only in the configured dimension;
* a malformed piggybacked coordinate is dropped and logged, not raised and
  not swallowed;
* an unconfirmed peer's timeout stretches with this node's load alone.
"""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.models.coordinates import NetworkCoordinate, VivaldiConfig
from hyperscale.distributed.models.distributed import NodeRole
from hyperscale.distributed.swim.coordinates.coordinate_tracker import CoordinateTracker
from hyperscale.distributed.swim.health_aware_server import HealthAwareServer
from hyperscale.distributed.swim.roles.confirmation_manager import RoleAwareConfirmationManager
from hyperscale.distributed.swim.roles.confirmation_strategy import get_strategy_for_role

LINK_RTT_MS = 50.0


def exchange_samples(rounds: int) -> tuple[CoordinateTracker, CoordinateTracker]:
    left = CoordinateTracker(config=VivaldiConfig())
    right = CoordinateTracker(config=VivaldiConfig())
    for _ in range(rounds):
        left.update_peer_coordinate("right", right.get_coordinate(), LINK_RTT_MS)
        right.update_peer_coordinate("left", left.get_coordinate(), LINK_RTT_MS)
    return left, right


def test_converged_coordinates_bound_the_rtt_tightly() -> None:
    left, right = exchange_samples(rounds=40)

    assert left.is_converged()
    assert left.estimate_rtt_ms(right.get_coordinate()) == pytest.approx(LINK_RTT_MS, rel=0.02)
    assert LINK_RTT_MS * 0.98 <= left.estimate_rtt_ucb_ms(right.get_coordinate()) < LINK_RTT_MS * 1.1


def test_a_coordinate_converges_only_with_the_samples_and_error_of_full_quality() -> None:
    config = VivaldiConfig()
    early, early_peer = exchange_samples(rounds=config.min_samples_for_routing - 1)
    settled, settled_peer = exchange_samples(rounds=config.min_samples_for_routing * 3)

    assert not early.is_converged()
    assert settled.is_converged()
    assert settled.coordinate_quality(settled.get_coordinate()) == pytest.approx(1.0)


def test_a_coordinate_round_trips_through_its_wire_form() -> None:
    left, _ = exchange_samples(rounds=12)
    coordinate = left.get_coordinate()

    decoded = NetworkCoordinate.from_dict(coordinate.to_dict())

    assert (decoded.vec, decoded.height, decoded.adjustment, decoded.error, decoded.sample_count) == (
        coordinate.vec,
        coordinate.height,
        coordinate.adjustment,
        coordinate.error,
        coordinate.sample_count,
    )


@pytest.mark.parametrize(
    ("wire_form", "error_type"),
    [
        ({"height": 0.0, "adjustment": 0.0, "error": 0.1, "sample_count": 1}, KeyError),
        ({"vec": [0.0] * 8, "height": 0.0, "adjustment": 0.0, "sample_count": 1}, KeyError),
        ({"vec": ["east"] * 8, "height": 0.0, "adjustment": 0.0, "error": 0.1, "sample_count": 1}, ValueError),
        ([0.0] * 8, TypeError),
    ],
)
def test_a_coordinate_decodes_only_whole(wire_form: object, error_type: type[Exception]) -> None:
    with pytest.raises(error_type):
        NetworkCoordinate.from_dict(wire_form)


def test_the_tracker_refuses_a_coordinate_of_another_dimension() -> None:
    tracker = CoordinateTracker(config=VivaldiConfig())
    flat = NetworkCoordinate(vec=[0.01, 0.02], height=0.0, adjustment=0.0, error=0.1)

    with pytest.raises(ValueError):
        tracker.update_peer_coordinate("peer", flat, LINK_RTT_MS)
    with pytest.raises(ValueError):
        tracker.record_peer_coordinate("peer", flat)

    assert tracker.get_peer_count() == 0
    assert tracker.get_coordinate().sample_count == 0


def make_piggyback_receiver() -> tuple[HealthAwareServer, list[object]]:
    """The parts of a SWIM server the Vivaldi piggyback path reads."""
    logged: list[object] = []
    server = object.__new__(HealthAwareServer)
    server._vivaldi_config = VivaldiConfig()
    server._coordinate_tracker = CoordinateTracker(config=server._vivaldi_config)
    server._pending_probe_start = {}
    server._clock = SimpleNamespace(monotonic=lambda: 100.0)


    async def log(entry: object) -> None:
        logged.append(entry)

    server._udp_logger = SimpleNamespace(log=log)
    server._host = "127.0.0.1"
    server._udp_port = 9001
    server._node_id = SimpleNamespace(full="node-under-test")
    return server, logged


@pytest.mark.parametrize(
    "piggyback",
    [b"not json", b"[1, 2, 3]", b'{"vec": [0.0, 0.0], "height": 0, "adjustment": 0, "error": 0.1, "sample_count": 1}'],
)
async def test_a_malformed_piggybacked_coordinate_is_logged_and_dropped(piggyback: bytes) -> None:
    server, logged = make_piggyback_receiver()

    await server._process_vivaldi_piggyback(piggyback, ("10.0.0.2", 9001))

    assert len(logged) == 1
    assert "Malformed Vivaldi coordinate from 10.0.0.2:9001" in logged[0].message
    assert server._coordinate_tracker.get_peer_count() == 0


async def test_a_piggybacked_coordinate_without_a_probe_is_remembered_without_moving_ours() -> None:
    server, logged = make_piggyback_receiver()
    peer, _ = exchange_samples(rounds=12)

    await server._process_vivaldi_piggyback(
        json.dumps(peer.get_coordinate().to_dict()).encode(),
        ("10.0.0.2", 9001),
    )

    assert logged == []
    assert server._coordinate_tracker.get_peer_coordinate("10.0.0.2:9001") is not None
    assert server._coordinate_tracker.get_coordinate().sample_count == 0


@pytest.mark.parametrize(
    ("role", "load_multiplier"),
    [(NodeRole.GATE, 2.0), (NodeRole.GATE, 50.0), (NodeRole.MANAGER, 1.0), (NodeRole.WORKER, 4.0)],
)
def test_an_unconfirmed_peers_timeout_stretches_with_load_alone(role: NodeRole, load_multiplier: float) -> None:
    strategy = get_strategy_for_role(role)
    manager = RoleAwareConfirmationManager(get_lhm_multiplier=lambda: load_multiplier)

    assert manager._calculate_effective_timeout(strategy) == pytest.approx(
        strategy.passive_timeout_seconds * min(load_multiplier, strategy.load_multiplier_max)
    )
