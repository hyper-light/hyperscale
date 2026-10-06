"""``HashRingNode`` -- pickled under the namespace
``hyperscale.distributed.jobs.gates.consistent_hash_ring`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class HashRingNode:
    """A node in the consistent hash ring."""

    node_id: str
    tcp_host: str
    tcp_port: int
    weight: int = 1
