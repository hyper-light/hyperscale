"""
Integration test for multi-node Raft consensus.

Uses a MockNetwork to simulate message passing between 3 RaftNodes.
Tests the full consensus lifecycle:
1. Leader election across multiple nodes
2. Log replication and commit advancement
3. Follower catch-up
4. Leader step-down on higher term
5. Re-election after leader loss
"""

import asyncio
import time
from collections import defaultdict
from unittest.mock import MagicMock

from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import RaftNode


class MockNetwork:
    """
    Simulates message delivery between RaftNodes.

    Messages are queued and delivered explicitly, allowing
    deterministic testing of the consensus protocol.
    """

    __slots__ = ("_nodes", "_message_queue", "_addrs", "_applied_entries")

    def __init__(self) -> None:
        self._nodes: dict[str, RaftNode] = {}
        self._message_queue: list[tuple[str, object]] = []
        self._addrs: dict[tuple[str, int], str] = {}
        self._applied_entries: dict[str, list] = defaultdict(list)

    def add_node(self, node_id: str, addr: tuple[str, int], node: RaftNode) -> None:
        """Register a node in the network."""
        self._nodes[node_id] = node
        self._addrs[addr] = node_id

    def make_send_callback(self, sender_id: str):
        """Create a send_message callback for a specific node."""

        async def send_message(addr: tuple[str, int], message: object) -> None:
            target_id = self._addrs.get(addr)
            if target_id is not None:
                self._message_queue.append((target_id, message))

        return send_message

    def make_apply_callback(self, node_id: str):
        """Create an apply_command callback for a specific node."""

        async def apply_command(entry) -> None:
            self._applied_entries[node_id].append(entry)

        return apply_command

    async def deliver_all_messages(self) -> int:
        """
        Deliver all queued messages to their target nodes.

        Returns the number of messages delivered.
        """
        delivered = 0
        while self._message_queue:
            target_id, message = self._message_queue.pop(0)
            node = self._nodes.get(target_id)
            if node is None:
                continue

            response = await self._dispatch_message(node, message)
            if response is not None:
                self._route_response(target_id, message, response)
            delivered += 1

        return delivered

    async def _dispatch_message(self, node: RaftNode, message: object) -> object | None:
        """Dispatch a message to the appropriate handler."""
        match message:
            case RequestVote():
                return await node.handle_request_vote(message)
            case RequestVoteResponse():
                await node.handle_request_vote_response(message)
                return None
            case AppendEntries():
                return await node.handle_append_entries(message)
            case AppendEntriesResponse():
                await node.handle_append_entries_response(message)
                return None
        return None

    def _route_response(
        self,
        responder_id: str,
        original: object,
        response: object,
    ) -> None:
        """Route a response back to the sender."""
        sender_id: str | None = None
        match original:
            case RequestVote():
                sender_id = original.candidate_id
            case AppendEntries():
                sender_id = original.leader_id
        if sender_id is not None:
            self._message_queue.append((sender_id, response))


def create_cluster(
    node_ids: list[str] | None = None,
) -> tuple[MockNetwork, dict[str, RaftNode]]:
    """
    Create a 3-node Raft cluster with mock networking.

    Returns (network, nodes_dict).
    """
    if node_ids is None:
        node_ids = ["node-1", "node-2", "node-3"]

    network = MockNetwork()
    members = set(node_ids)
    member_addrs = {
        node_id: ("127.0.0.1", 9001 + index)
        for index, node_id in enumerate(node_ids)
    }

    nodes: dict[str, RaftNode] = {}
    for node_id in node_ids:
        logger_mock = MagicMock()
        logger_mock.log = MagicMock(return_value=asyncio.coroutine(lambda: None)())

        node = RaftNode(
            job_id="job-1",
            node_id=node_id,
            members=members,
            member_addrs=member_addrs,
            send_message=network.make_send_callback(node_id),
            apply_command=network.make_apply_callback(node_id),
            on_become_leader=None,
            on_lose_leadership=None,
            logger=logger_mock,
        )
        addr = member_addrs[node_id]
        network.add_node(node_id, addr, node)
        nodes[node_id] = node

    return network, nodes


def find_leader(nodes: dict[str, RaftNode]) -> str | None:
    """Find the current leader among nodes. Returns None if no leader."""
    for node_id, node in nodes.items():
        if node.is_leader():
            return node_id
    return None


def count_role(nodes: dict[str, RaftNode], role: str) -> int:
    """Count nodes with a given role."""
    return sum(1 for node in nodes.values() if node.role == role)


# =============================================================================
# Tests
# =============================================================================


async def test_leader_election_three_nodes() -> None:
    """A 3-node cluster should elect exactly one leader."""
    network, nodes = create_cluster()

    # Trigger election on node-1
    await nodes["node-1"].start_election()
    await network.deliver_all_messages()
    # Deliver vote responses
    await network.deliver_all_messages()

    leader_id = find_leader(nodes)
    assert leader_id == "node-1"
    assert count_role(nodes, "leader") == 1


async def test_log_replication() -> None:
    """Leader should replicate log entries to followers."""
    network, nodes = create_cluster()

    # Elect node-1
    await nodes["node-1"].start_election()
    await network.deliver_all_messages()
    await network.deliver_all_messages()
    assert nodes["node-1"].is_leader()

    # Propose a command
    success, index = await nodes["node-1"].propose(b"command-1", "CREATE_JOB")
    assert success is True
    assert index == 1

    # Replicate to followers
    await nodes["node-1"].replicate_to_followers()
    await network.deliver_all_messages()
    # Deliver AppendEntries responses
    await network.deliver_all_messages()

    # Leader should have advanced commit
    assert nodes["node-1"].commit_index == 1


async def test_follower_applies_committed_entries() -> None:
    """Followers should apply entries once committed."""
    network, nodes = create_cluster()

    # Elect and propose
    await nodes["node-1"].start_election()
    await network.deliver_all_messages()
    await network.deliver_all_messages()

    await nodes["node-1"].propose(b"cmd-1", "CREATE_JOB")
    await nodes["node-1"].replicate_to_followers()
    await network.deliver_all_messages()
    await network.deliver_all_messages()

    # Leader sends another heartbeat with updated commit index
    await nodes["node-1"].replicate_to_followers()
    await network.deliver_all_messages()
    await network.deliver_all_messages()

    # Followers should now have commit_index = 1
    for node_id in ["node-2", "node-3"]:
        assert nodes[node_id].commit_index == 1

    # Apply on followers
    for node_id in ["node-2", "node-3"]:
        applied = await nodes[node_id].apply_committed_entries()
        assert applied == 1


async def test_multiple_proposals() -> None:
    """Multiple proposals should all be replicated and committed."""
    network, nodes = create_cluster()

    await nodes["node-1"].start_election()
    await network.deliver_all_messages()
    await network.deliver_all_messages()

    # Propose 5 commands
    for idx in range(5):
        success, _ = await nodes["node-1"].propose(
            f"cmd-{idx}".encode(), "CREATE_JOB"
        )
        assert success is True

    # Replicate and deliver
    await nodes["node-1"].replicate_to_followers()
    await network.deliver_all_messages()
    await network.deliver_all_messages()

    assert nodes["node-1"].commit_index == 5

    # Apply on leader
    applied = await nodes["node-1"].apply_committed_entries()
    assert applied == 5


async def test_step_down_on_higher_term() -> None:
    """A leader should step down when it sees a higher term."""
    network, nodes = create_cluster()

    # Elect node-1
    await nodes["node-1"].start_election()
    await network.deliver_all_messages()
    await network.deliver_all_messages()
    assert nodes["node-1"].is_leader()

    # node-2 starts election with higher term (simulating network partition recovery)
    await nodes["node-2"].start_election()
    # Deliver vote requests (including to node-1 which will step down)
    await network.deliver_all_messages()
    await network.deliver_all_messages()

    # node-1 should have stepped down
    assert not nodes["node-1"].is_leader()


async def test_re_election_after_leader_loss() -> None:
    """Cluster should elect a new leader after the old one is destroyed."""
    network, nodes = create_cluster()

    # Elect node-1
    await nodes["node-1"].start_election()
    await network.deliver_all_messages()
    await network.deliver_all_messages()
    assert nodes["node-1"].is_leader()

    # Destroy leader (simulate crash)
    nodes["node-1"].destroy()

    # node-2 starts election
    await nodes["node-2"].start_election()
    await network.deliver_all_messages()
    await network.deliver_all_messages()

    # node-2 should be new leader (node-1 destroyed, node-3 votes)
    assert nodes["node-2"].is_leader()
    assert nodes["node-2"].current_term == 2


async def test_cleanup_releases_all_state() -> None:
    """After destroy(), all nodes should release their state."""
    network, nodes = create_cluster()

    await nodes["node-1"].start_election()
    await network.deliver_all_messages()
    await network.deliver_all_messages()

    await nodes["node-1"].propose(b"cmd", "CREATE_JOB")

    for node in nodes.values():
        node.destroy()

    # All operations should be no-ops
    for node in nodes.values():
        success, _ = await node.propose(b"should-fail", "CREATE_JOB")
        assert success is False
        applied = await node.apply_committed_entries()
        assert applied == 0


async def test_single_node_consensus() -> None:
    """A single-node cluster should work correctly."""
    network, nodes = create_cluster(node_ids=["solo"])

    await nodes["solo"].start_election()
    assert nodes["solo"].is_leader()
    assert nodes["solo"].current_term == 1

    success, index = await nodes["solo"].propose(b"cmd", "CREATE_JOB")
    assert success is True
    assert index == 1

    # Single node commits immediately (quorum = 1)
    applied = await nodes["solo"].apply_committed_entries()
    assert applied == 1


if __name__ == "__main__":
    print("Running Raft consensus integration tests...")

    tests = [
        ("Leader election (3 nodes)", test_leader_election_three_nodes),
        ("Log replication", test_log_replication),
        ("Follower applies committed", test_follower_applies_committed_entries),
        ("Multiple proposals", test_multiple_proposals),
        ("Step down on higher term", test_step_down_on_higher_term),
        ("Re-election after leader loss", test_re_election_after_leader_loss),
        ("Cleanup releases state", test_cleanup_releases_all_state),
        ("Single node consensus", test_single_node_consensus),
    ]

    passed = 0
    failed = 0
    for name, test_func in tests:
        try:
            asyncio.run(test_func())
            print(f"  PASS: {name}")
            passed += 1
        except Exception as error:
            print(f"  FAIL: {name} -- {error}")
            failed += 1

    print(f"\nResults: {passed} passed, {failed} failed out of {len(tests)} tests")
