"""
Suspect gossip carries its original accuser so receivers can count
Lifeguard confirmations (Dadgar et al., "Lifeguard: Local Health Awareness
for More Accurate Failure Detection", §IV-B; memberlist ``suspectNode``).

* wire: a suspect entry is followed by a ``by:<inc>:<host>:<port>`` entry;
  a node running the previous parser (frozen below, verbatim from
  a2142212) still decodes the suspect entry unchanged and sees the
  annotation as an unknown-type update, which it suppresses; the new
  parser decodes old-format entries with the accuser unknown;
* the MTU cap still holds at the maximum address length, and an
  annotation never travels without its suspect entry;
* counting: once per distinct accuser, never from an unknown accuser,
  never from the originator.
"""

import sys

from hyperscale.distributed.swim.detection.suspicion_state import SuspicionState
from hyperscale.distributed.swim.gossip.gossip_buffer import GossipBuffer
from hyperscale.distributed.swim.gossip.piggyback_update import (
    ACCUSER_UPDATE_TYPE,
    PiggybackUpdate,
)

DEAD_GATE = ("sim-gate-c", 9001)
ACCUSER = ("sim-gate-b", 9001)
OTHER_ACCUSER = ("sim-mgr", 9001)
NODE_ID = "global-50-sim-gate-c-09001-0000000000000"
# RFC 1035 section 2.3.4: a domain name is at most 253 characters in text.
MAX_HOST = "h" * 253
MAX_PORT = 65535
# The largest incarnation an int64 counter reaches.
MAX_INCARNATION = 2**63 - 1


def previous_from_bytes(data: bytes) -> tuple | None:
    """The a2142212 ``PiggybackUpdate.from_bytes`` field split, verbatim."""
    try:
        parts = data.decode().split(":", maxsplit=5)
        if len(parts) < 4:
            return None
        return (
            parts[0],
            int(parts[1]),
            sys.intern(parts[2]),
            int(parts[3]),
            parts[4] if len(parts) >= 5 and parts[4] else None,
            parts[5] if len(parts) >= 6 and parts[5] else None,
        )
    except (ValueError, UnicodeDecodeError):
        return None


def previous_decode(data: bytes) -> list[tuple]:
    """The a2142212 ``GossipBuffer.decode_piggyback`` loop, verbatim."""
    decoded = []
    for part in data[3:].split(b"|"):
        if part and (update := previous_from_bytes(part)):
            decoded.append(update)
    return decoded


def suspect_with_accuser(accuser: tuple[str, int] | None) -> GossipBuffer:
    buffer = GossipBuffer()
    buffer.add_update(
        "suspect", DEAD_GATE, incarnation=12, n_members=4, role="gate", node_id=NODE_ID, accuser=accuser
    )
    return buffer


def test_a_previous_node_reads_the_suspect_entry_unchanged_and_sees_an_unknown_annotation() -> None:
    piggyback = suspect_with_accuser(ACCUSER).encode_piggyback()

    suspect_entry, annotation = previous_decode(piggyback)

    assert suspect_entry == ("suspect", 12, "sim-gate-c", 9001, "gate", NODE_ID)
    # Its process_piggyback_data maps no status for this type and
    # suppresses it (gossip_unknown_updates_suppressed).
    assert annotation[0] == ACCUSER_UPDATE_TYPE


def test_a_new_node_attributes_new_entries_and_leaves_old_ones_unattributed() -> None:
    old_entry = b"suspect:7:sim-wkr:9001:worker"
    new_entries = suspect_with_accuser(ACCUSER).encode_piggyback()[3:]
    mixed = GossipBuffer.MEMBERSHIP_SEPARATOR + old_entry + b"|" + new_entries + b"|alive:3:sim-mgr:9001"

    old_update, new_update, alive_update = GossipBuffer.decode_piggyback(mixed)

    assert (old_update.node, old_update.accuser) == (("sim-wkr", 9001), None)
    assert (new_update.node, new_update.node_id, new_update.accuser) == (DEAD_GATE, NODE_ID, ACCUSER)
    assert (alive_update.update_type, alive_update.accuser) == ("alive", None)


def test_an_annotation_leading_the_section_stays_an_unknown_update() -> None:
    stray = GossipBuffer.MEMBERSHIP_SEPARATOR + b"by:12:sim-gate-b:9001"

    (update,) = GossipBuffer.decode_piggyback(stray)

    assert update.update_type == ACCUSER_UPDATE_TYPE


def test_the_annotation_size_at_the_maximum_address_length_stays_inside_the_cap() -> None:
    plain = PiggybackUpdate("suspect", (MAX_HOST, MAX_PORT), MAX_INCARNATION, 0.0)
    attributed = PiggybackUpdate(
        "suspect", (MAX_HOST, MAX_PORT), MAX_INCARNATION, 0.0, accuser=(MAX_HOST, MAX_PORT)
    )

    annotation_bytes = len(attributed.to_bytes()) - len(plain.to_bytes())

    # "|by:" + 19-digit incarnation + ":" + 253-char host + ":" + 5-digit port.
    assert annotation_bytes == 4 + 19 + 1 + 253 + 1 + 5
    buffer = GossipBuffer()
    buffer.add_update("suspect", (MAX_HOST, MAX_PORT), MAX_INCARNATION, accuser=(MAX_HOST, MAX_PORT))
    for max_size in range(len(attributed.to_bytes()) - 5, len(attributed.to_bytes()) + 10):
        piggyback = buffer.encode_piggyback(max_size=max_size)
        assert len(piggyback) <= max_size
        # All or nothing: the annotation never leaves without its entry.
        assert piggyback in (b"", GossipBuffer.MEMBERSHIP_SEPARATOR + attributed.to_bytes())


def test_the_same_accusation_requeued_keeps_its_broadcast_count() -> None:
    buffer = suspect_with_accuser(ACCUSER)
    buffer.encode_piggyback()

    assert not buffer.add_update("suspect", DEAD_GATE, incarnation=12, accuser=ACCUSER)
    assert buffer.updates[DEAD_GATE].broadcast_count == 1


def test_a_different_accusers_suspicion_replaces_the_relayed_one() -> None:
    buffer = suspect_with_accuser(ACCUSER)
    buffer.encode_piggyback()

    assert buffer.add_update("suspect", DEAD_GATE, incarnation=12, accuser=OTHER_ACCUSER)
    assert (buffer.updates[DEAD_GATE].accuser, buffer.updates[DEAD_GATE].broadcast_count) == (OTHER_ACCUSER, 0)


def test_status_and_incarnation_precedence_is_unchanged() -> None:
    buffer = suspect_with_accuser(ACCUSER)

    assert not buffer.add_update("alive", DEAD_GATE, incarnation=12)
    assert not buffer.add_update("suspect", DEAD_GATE, incarnation=11, accuser=OTHER_ACCUSER)
    assert buffer.add_update("dead", DEAD_GATE, incarnation=12)
    assert not buffer.add_update("suspect", DEAD_GATE, incarnation=12, accuser=OTHER_ACCUSER)
    assert buffer.add_update("alive", DEAD_GATE, incarnation=13)


def test_confirmations_count_once_per_distinct_accuser_and_never_unattributed() -> None:
    state = SuspicionState(
        node=DEAD_GATE,
        incarnation=12,
        start_time=0.0,
        originator=ACCUSER,
        min_timeout=30.0,
        max_timeout=120.0,
        required_confirmations=2,
    )

    assert not state.add_confirmation(None)
    assert not state.add_confirmation(ACCUSER)
    assert state.calculate_timeout() == 120.0
    assert state.add_confirmation(OTHER_ACCUSER)
    assert not state.add_confirmation(OTHER_ACCUSER)
    # Lifeguard: max - (max - min) * log(C + 1) / log(K + 1), C=1, K=2.
    assert abs(state.calculate_timeout() - (120.0 - 90.0 * 0.6309297535714574)) < 1e-9
