from __future__ import annotations

import hashlib

from hyperscale.distributed.hlc.hlc_timestamp import NODE_ID_BITS


def hlc_node_id(stable_identity: str) -> int:
    """The HLC node id for a node's stable identity (role, datacenter,
    host, port -- not a per-process value): the same across restarts,
    and distinct across nodes except with probability ~n^2 / 2^65.

    ``hash()`` is randomized per process, so it gave a restarted node a
    new id and colliding 16-bit ids across a cluster.
    """
    return int.from_bytes(
        hashlib.blake2b(stable_identity.encode(), digest_size=NODE_ID_BITS // 8).digest(),
        "big",
    )
