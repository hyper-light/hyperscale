"""
NodeId is topology-derived: identity, ordering, and hashing are a pure
function of ``(datacenter, priority, host, port)`` — no wall clock, no
randomness. ``created_ms`` is retained only as non-ordered metadata.

This is the contract that makes replay deterministic (two runs of the
same topology produce identical identities) and leadership + the
cross-gate job hash-ring topology-stable. These tests pin it so the
``uuid.uuid4()`` / wall-clock identity can never creep back.
"""

from hyperscale.distributed.swim.core.node_id import NodeId


def test_identity_is_deterministic_given_placement():
    """Two NodeIds for the same placement are equal and hash-equal —
    there is no per-construction randomness."""
    first = NodeId.generate("dc-east", 50, host="10.0.0.4", port=9001)
    second = NodeId.generate("dc-east", 50, host="10.0.0.4", port=9001)

    assert first == second
    assert hash(first) == hash(second)


def test_created_ms_is_metadata_only():
    """A different ``created_ms`` must not change identity, hash, or
    order — it is observability metadata, excluded from the ordered
    key."""
    early = NodeId(datacenter="dc", priority=50, host="h", port=9001, created_ms=1000)
    late = NodeId(datacenter="dc", priority=50, host="h", port=9001, created_ms=9999)

    assert early == late
    assert hash(early) == hash(late)
    assert not (early < late) and not (late < early)
    # ...but it IS visible in the human-readable string.
    assert early.full != late.full


def test_ordering_is_topology_lexicographic():
    """Order is (datacenter, priority, host, port): priority is the
    intentional leadership knob, host:port the deterministic tie-break
    beneath it — no wall clock, no randomness participates."""
    # priority dominates host/port.
    assert NodeId.generate("dc", 10, host="10.0.0.9", port=9999) < NodeId.generate(
        "dc", 50, host="10.0.0.1", port=9001
    )
    # equal priority -> host then port breaks the tie, stably.
    assert NodeId.generate("dc", 50, host="10.0.0.1", port=9001) < NodeId.generate(
        "dc", 50, host="10.0.0.1", port=9002
    )
    # datacenter is the outermost key.
    assert NodeId.generate("dc-a", 50, host="h", port=9001) < NodeId.generate(
        "dc-b", 50, host="h", port=9001
    )


def test_ordering_is_stable_across_restart():
    """A node that 'restarts' (fresh created_ms) keeps its exact
    leadership rank — the property topology identity exists to provide."""
    before = NodeId(datacenter="dc", priority=50, host="h", port=9001, created_ms=1)
    peer = NodeId(datacenter="dc", priority=50, host="h", port=9002, created_ms=1)
    after_restart = NodeId(
        datacenter="dc", priority=50, host="h", port=9001, created_ms=10_000
    )

    assert before < peer
    # Restart did not flip the ordering, despite a much later created_ms.
    assert after_restart < peer


def test_string_round_trips_through_parse():
    node_id = NodeId.generate("DC-EAST", 1, host="10.0.0.4", port=9000)
    assert NodeId.parse(node_id.full) == node_id
    assert NodeId.parse(node_id.full).full == node_id.full


def test_string_sorts_consistently_with_object_order():
    """The zero-padded string form sorts the same way the objects do, so
    code that happens to sort id STRINGS agrees with object ordering."""
    ids = [
        NodeId.generate("dc", 50, host="10.0.0.1", port=9002),
        NodeId.generate("dc", 10, host="10.0.0.9", port=9001),
        NodeId.generate("dc", 50, host="10.0.0.1", port=9001),
    ]
    by_object = sorted(ids)
    by_string = sorted(ids, key=lambda n: n.full)
    assert by_object == by_string


def test_generate_requires_host_and_port():
    """host/port are the identity — they cannot be omitted."""
    import pytest

    with pytest.raises(TypeError):
        NodeId.generate("dc", 50)


def test_validation_rejects_bad_components():
    import pytest

    with pytest.raises(ValueError):
        NodeId(datacenter="", priority=50, host="h", port=9001)
    with pytest.raises(ValueError):
        NodeId(datacenter="dc", priority=100, host="h", port=9001)
    with pytest.raises(ValueError):
        NodeId(datacenter="dc", priority=50, host="", port=9001)
    with pytest.raises(ValueError):
        NodeId(datacenter="dc", priority=50, host="h", port=70000)
