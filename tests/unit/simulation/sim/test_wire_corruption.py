"""
A8 on-wire corruption at the coordinator's ``_enqueue`` chokepoint —
``schedule_corrupt`` flips one seeded byte of a matching DATAGRAM
payload and delivers the frame corrupt (the reject-path fault: auth/
parse must discard it whole, indistinguishable from loss).

These are pure-logic tests: every cross-process datagram funnels
through ``_enqueue``, so exercising the chokepoint directly proves the
knob's semantics — which frames flip, which byte, stream exemption,
window/link scoping — without spawning child processes. Determinism is
asserted the strong way: two identically seeded coordinators produce
IDENTICAL corrupted frames (same byte index, same flipped value), and a
different seed produces a different flip.

Streams are exempt by design: TCP checksums the wire, so a corrupted
segment is dropped by the kernel and retransmitted — on-wire corruption
never reaches a production TCP reader as delivered-corrupt bytes (its
only observable is added latency, which ``schedule_delay`` models).
"""

import pytest

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator

_SOURCE_ADDRESS = ("proc", 1)
_DESTINATION_ADDRESS = ("proc", 2)
_ADDRESS_MAP = {_SOURCE_ADDRESS: "A", _DESTINATION_ADDRESS: "B"}
_PAYLOAD = b"hello-corruption-target"


def _enqueue_frames(coordinator: SimulationCoordinator, sends: list) -> list:
    """Push ``sends`` — ``(send_time, src, dst, payload)`` — through the
    chokepoint; return the pending deliveries sorted by (time, seq)."""
    pending: list = []
    sequence = 0
    for send_time, source_address, destination_address, payload in sends:
        sequence = coordinator._enqueue(
            pending,
            sequence,
            _ADDRESS_MAP,
            send_time,
            source_address,
            destination_address,
            payload,
        )
    return sorted(pending)


def _delivered_payloads(pending: list) -> list:
    return [delivery[5] for delivery in pending]


def test_corrupt_flips_exactly_one_byte_of_matching_datagram():
    coordinator = SimulationCoordinator(latency=0.5, seed=7)
    coordinator.schedule_corrupt("A", "B", probability=1.0)

    pending = _enqueue_frames(
        coordinator,
        [(1.0, _SOURCE_ADDRESS, _DESTINATION_ADDRESS, ("dgram", _PAYLOAD))],
    )

    assert len(pending) == 1
    tag, delivered_payload = pending[0][5]
    assert tag == "dgram"
    assert delivered_payload != _PAYLOAD
    assert len(delivered_payload) == len(_PAYLOAD)
    differing_indexes = [
        index
        for index in range(len(_PAYLOAD))
        if delivered_payload[index] != _PAYLOAD[index]
    ]
    assert len(differing_indexes) == 1, differing_indexes


def test_corruption_is_deterministic_per_seed():
    """Same seed: identical flips (same byte, same value) across every
    frame. Different seed: a different corruption pattern."""
    sends = [
        (float(send_index), _SOURCE_ADDRESS, _DESTINATION_ADDRESS,
         ("dgram", _PAYLOAD))
        for send_index in range(1, 6)
    ]

    def corrupted_run(seed: int) -> list:
        coordinator = SimulationCoordinator(latency=0.5, seed=seed)
        coordinator.schedule_corrupt("A", "B", probability=1.0)
        return _delivered_payloads(_enqueue_frames(coordinator, sends))

    first_run = corrupted_run(7)
    second_run = corrupted_run(7)
    other_seed_run = corrupted_run(8)

    assert first_run == second_run
    assert first_run != other_seed_run
    assert all(payload[1] != _PAYLOAD for payload in first_run)


def test_streams_are_exempt_from_corruption():
    """Stream frames pass the chokepoint bit-identical even under a
    wildcard always-corrupt rule — the documented TCP-checksum
    exemption."""
    coordinator = SimulationCoordinator(latency=0.5, seed=7)
    coordinator.schedule_corrupt(None, None, probability=1.0)

    stream_payloads = [
        ("s-conn", (_SOURCE_ADDRESS, 0)),
        ("s-data", (_SOURCE_ADDRESS, 0), b"stream-bytes-stay-intact"),
        ("s-close", (_SOURCE_ADDRESS, 0)),
    ]
    pending = _enqueue_frames(
        coordinator,
        [
            (1.0, _SOURCE_ADDRESS, _DESTINATION_ADDRESS, payload)
            for payload in stream_payloads
        ],
    )

    assert _delivered_payloads(pending) == stream_payloads


def test_corruption_respects_window_and_link_scope():
    """Only frames sent inside ``[at_time, until_time)`` on the ruled
    link corrupt; everything else is delivered intact."""
    coordinator = SimulationCoordinator(latency=0.5, seed=7)
    coordinator.schedule_corrupt(
        "A", "B", probability=1.0, at_time=10.0, until_time=20.0
    )

    pending = _enqueue_frames(
        coordinator,
        [
            (5.0, _SOURCE_ADDRESS, _DESTINATION_ADDRESS, ("dgram", _PAYLOAD)),
            (15.0, _SOURCE_ADDRESS, _DESTINATION_ADDRESS, ("dgram", _PAYLOAD)),
            (15.0, _DESTINATION_ADDRESS, _SOURCE_ADDRESS, ("dgram", _PAYLOAD)),
            (25.0, _SOURCE_ADDRESS, _DESTINATION_ADDRESS, ("dgram", _PAYLOAD)),
        ],
    )

    forward_link_payloads = {
        delivery[0]: delivery[5][1]
        for delivery in pending
        if delivery[2] == "B"
    }
    assert forward_link_payloads[5.5] == _PAYLOAD
    assert forward_link_payloads[15.5] != _PAYLOAD
    assert forward_link_payloads[25.5] == _PAYLOAD
    reverse_link_payloads = [
        delivery[5][1] for delivery in pending if delivery[2] == "A"
    ]
    assert reverse_link_payloads == [_PAYLOAD]


def test_duplicate_of_corrupted_frame_carries_same_bytes():
    """Corruption happens once per on-wire frame; a drawn duplicate is
    a network-level copy of the SAME (corrupted) frame."""
    coordinator = SimulationCoordinator(latency=0.5, seed=7)
    coordinator.schedule_corrupt("A", "B", probability=1.0)
    coordinator.schedule_duplicate("A", "B", probability=1.0)

    pending = _enqueue_frames(
        coordinator,
        [(1.0, _SOURCE_ADDRESS, _DESTINATION_ADDRESS, ("dgram", _PAYLOAD))],
    )

    assert len(pending) == 2
    first_payload, duplicate_payload = _delivered_payloads(pending)
    assert first_payload == duplicate_payload
    assert first_payload[1] != _PAYLOAD


def test_empty_datagram_passes_unchanged():
    """A zero-length payload has no bytes to rot — delivered intact."""
    coordinator = SimulationCoordinator(latency=0.5, seed=7)
    coordinator.schedule_corrupt("A", "B", probability=1.0)

    pending = _enqueue_frames(
        coordinator,
        [(1.0, _SOURCE_ADDRESS, _DESTINATION_ADDRESS, ("dgram", b""))],
    )

    assert _delivered_payloads(pending) == [("dgram", b"")]


def test_corrupt_probability_validation():
    coordinator = SimulationCoordinator(latency=0.5, seed=7)
    with pytest.raises(ValueError):
        coordinator.schedule_corrupt("A", "B", probability=1.5)
    with pytest.raises(ValueError):
        coordinator.schedule_corrupt("A", "B", probability=-0.1)
