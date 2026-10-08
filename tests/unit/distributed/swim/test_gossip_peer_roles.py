"""
A node never records itself as its own peer.

Gossip about a node reaches the node itself -- peers gossip its
membership, their suspicion of it, its refutations -- and every such
update carries its role. The piggyback path recorded that role for the
node's own address, so a node counted itself twice in its leader
election: a three-gate tier counted four and needed all three gates to
elect, so a single unreachable gate left the tier unable to elect.

* gossip about the node itself leaves its peer roles alone, while
  gossip about its peers records theirs;
* the node's election cohort counts it once.
"""

import pytest

from hyperscale.distributed.models.distributed import NodeRole
from hyperscale.distributed.swim.gossip.gossip_buffer import GossipBuffer
from hyperscale.distributed.swim.health_aware_server import HealthAwareServer

SELF_ADDR = ("10.0.0.1", 9001)
PEER_ADDRS = [("10.0.0.2", 9001), ("10.0.0.3", 9001)]


class DiscardingMetrics:
    def increment(self, name: str, amount: int = 1) -> None:
        pass


def make_gate() -> HealthAwareServer:
    gate = object.__new__(HealthAwareServer)
    gate._udp_addr_slug = f"{SELF_ADDR[0]}:{SELF_ADDR[1]}".encode()
    gate._node_role = "gate"
    gate._peer_roles = {}
    gate._metrics = DiscardingMetrics()
    # Roles are recorded before an update's liveness is applied; the
    # liveness half is out of scope here, so every update stops there.
    gate._should_apply_liveness_piggyback = lambda update, source_addr: False
    return gate


def gossip_about(nodes: list[tuple[str, int]]) -> bytes:
    buffer = GossipBuffer()
    for node in nodes:
        buffer.add_update("alive", node, incarnation=1, n_members=len(nodes), role="gate")
    return buffer.encode_piggyback(max_count=len(nodes))


@pytest.mark.asyncio
async def test_gossip_about_the_node_itself_leaves_its_peer_roles_alone() -> None:
    gate = make_gate()

    await gate.process_piggyback_data(gossip_about([SELF_ADDR, *PEER_ADDRS]), source_addr=PEER_ADDRS[0])

    assert gate._peer_roles == {peer_addr: NodeRole.GATE for peer_addr in PEER_ADDRS}
    assert gate._get_election_member_count() == len(PEER_ADDRS) + 1
