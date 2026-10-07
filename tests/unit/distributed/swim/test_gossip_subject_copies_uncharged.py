"""
A membership update's relay budget is spent only on copies that can be
relayed: a copy sent to the update's own subject still carries the update
(an alive subject refutes from it) but costs no broadcast.

Measured before the fix (gate-fault long-horizon composite, seed 220): a
manager's DEAD update for a killed gate spent 3 of its 5 broadcasts on
messages to that dead gate itself — its in-flight probe round, the
suspicion notice, a proxy probe — so one surviving gate never heard.
"""

from hyperscale.distributed.swim.gossip.gossip_buffer import GossipBuffer

DEAD_GATE = ("sim-gate-c", 9001)
SURVIVOR = ("sim-gate-a", 9001)
MEMBER_COUNT = 4


def buffer_with_dead_update() -> GossipBuffer:
    buffer = GossipBuffer()
    buffer.add_update("dead", DEAD_GATE, incarnation=12, n_members=MEMBER_COUNT)
    return buffer


def test_copies_to_the_subject_carry_the_update_without_spending_its_budget() -> None:
    buffer = buffer_with_dead_update()
    max_broadcasts = buffer.updates[DEAD_GATE].max_broadcasts

    for _ in range(max_broadcasts * 2):
        assert b"dead:12:sim-gate-c:9001" in buffer.encode_piggyback(destination=DEAD_GATE)

    assert buffer.updates[DEAD_GATE].broadcast_count == 0


def test_copies_to_other_members_spend_the_budget_until_the_update_retires() -> None:
    buffer = buffer_with_dead_update()
    max_broadcasts = buffer.updates[DEAD_GATE].max_broadcasts

    for _ in range(max_broadcasts):
        assert b"dead:12:sim-gate-c:9001" in buffer.encode_piggyback(destination=SURVIVOR)

    assert DEAD_GATE not in buffer.updates
    assert buffer.encode_piggyback(destination=SURVIVOR) == b""


def test_a_copy_without_a_known_destination_is_charged() -> None:
    buffer = buffer_with_dead_update()

    buffer.encode_piggyback_with_base(b"ack>sim-mgr:9001")

    assert buffer.updates[DEAD_GATE].broadcast_count == 1


def test_the_subject_destination_is_threaded_through_the_base_aware_encoder() -> None:
    buffer = buffer_with_dead_update()

    buffer.encode_piggyback_with_base(b"probe:1>sim-gate-c:9001", destination=DEAD_GATE)

    assert buffer.updates[DEAD_GATE].broadcast_count == 0
