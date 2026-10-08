import msgspec


class KeyedStateRecord(msgspec.Struct, frozen=True, tag="keyed_state", array_like=True):
    """The whole durable state of one ``key`` in ``namespace`` -- consensus
    state a node keeps beside its Raft groups under the same identity, such
    as a gate's two-phase-commit votes on a job's replica. The record with
    the highest ``version`` of a key is its state: writes may reach the
    disk out of order, a higher version never loses to a lower one, and a
    version is written once. ``state`` is the owner's own msgspec encoding."""

    namespace: str
    key: str
    version: int
    state: bytes
