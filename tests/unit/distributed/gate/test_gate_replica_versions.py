"""
A job's replica is versioned by leadership epoch, then sequence.

The gate replica protocol ordered a job's replicas by sequence alone, and
a leader's revision went out one sequence above the replica it read:

* a takeover whose sequence did not exceed a revision the old leader had
  committed was answered "already committed" -- counted as committed --
  and never applied, while a revision the deposed leader sent later
  overwrote the new leader's replica;
* an abort of one epoch's revision rolled back another epoch's replica
  committed at the same sequence;
* a takeover was built from the taking-over gate's own copy, which may
  have missed the old leader's last revision: the older state was
  committed over it;
* two revisions of a job at once were built on the same replica: the
  later commit dropped the earlier one's change.

Now the fence (the leadership epoch) orders replicas first, aborts name
the exact version, revisions of a job run one at a time under sequences
never reused, and a takeover adopts the freshest replica a quorum holds.

Three real ``GateJobReplicationCoordinator`` instances -- gates a, b and c
-- exchange the protocol's messages in memory; links between them are cut
to partition them.
"""

import asyncio
import dataclasses
from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import (
    GateJobReplica,
    GateJobReplicaAbort,
    GateJobReplicaStatus,
)
from hyperscale.distributed.models.gate_replication import GateJobReplicaAck
from hyperscale.distributed.nodes.gate.replication_coordinator import (
    GateJobReplicationCoordinator,
)
from hyperscale.distributed.runtime import RealClock

JOB_ID = "job-1"
GATE_ADDRESSES = {
    "gate-a": ("10.0.0.1", 9000),
    "gate-b": ("10.0.0.2", 9000),
    "gate-c": ("10.0.0.3", 9000),
}
QUORUM = 2
HANDLERS = {
    "gate_job_replica_prepare": "handle_prepare",
    "gate_job_replica_commit": "handle_commit",
    "gate_job_replica_abort": "handle_abort",
    "gate_job_replica_fetch": "handle_fetch",
}


class SilentLogger:
    async def log(self, model) -> None:
        return None


class GateTier:
    """Three gates' coordinators, the replica each applied, and the links
    between them."""

    def __init__(self) -> None:
        self.cut_links: set[frozenset[str]] = set()
        self.applied: dict[str, GateJobReplica] = {}
        self.coordinators = {
            gate_id: self._build(gate_id) for gate_id in GATE_ADDRESSES
        }

    def _build(self, gate_id: str) -> GateJobReplicationCoordinator:
        async def send_tcp(peer_address, action, payload, timeout):
            peer_id = next(
                peer for peer, address in GATE_ADDRESSES.items() if address == peer_address
            )
            if frozenset((gate_id, peer_id)) in self.cut_links:
                return ConnectionRefusedError(f"{peer_id} is unreachable"), 0
            handler = getattr(self.coordinators[peer_id], HANDLERS[action])
            return await handler(payload), 0

        async def apply_committed(replica: GateJobReplica) -> None:
            self.applied[gate_id] = replica

        async def drop_committed(job_id: str) -> None:
            self.applied.pop(gate_id, None)

        return GateJobReplicationCoordinator(
            logger=SilentLogger(),
            task_runner=SimpleNamespace(run=lambda *args, **kwargs: None),
            get_node_id=lambda: SimpleNamespace(full=gate_id, short=gate_id),
            get_node_addr=lambda: GATE_ADDRESSES[gate_id],
            send_tcp=send_tcp,
            apply_committed=apply_committed,
            drop_committed=drop_committed,
            clock=RealClock(),
        )

    def peers_of(self, gate_id: str) -> list[tuple[str, int]]:
        return [address for peer, address in GATE_ADDRESSES.items() if peer != gate_id]

    def cut(self, gate_id: str, *peer_ids: str) -> None:
        self.cut_links.update(frozenset((gate_id, peer_id)) for peer_id in peer_ids)

    def heal(self) -> None:
        self.cut_links.clear()


def accepted_replica(leader_id: str) -> GateJobReplica:
    leader_address = GATE_ADDRESSES[leader_id]
    return GateJobReplica(
        job_id=JOB_ID,
        sequence=1,
        fence_token=1,
        leader_id=leader_id,
        leader_addr=leader_address,
        origin_gate_addr=leader_address,
        callback_addr=None,
        target_dcs=["dc-a"],
        target_dc_count=1,
        status_seed="submitted",
        submitted_wall_time=0.0,
        raft_voters=sorted(GATE_ADDRESSES),
    )


def taken_over_by(tier: GateTier, gate_id: str):
    """The takeover a gate builds from the replica it applied."""

    def build_takeover() -> GateJobReplica:
        current = tier.applied[gate_id]
        return dataclasses.replace(
            current,
            fence_token=current.fence_token + 1,
            leader_id=gate_id,
            leader_addr=GATE_ADDRESSES[gate_id],
        )

    return build_takeover


def moved_to(datacenter: str):
    return lambda committed: dataclasses.replace(committed, target_dcs=[datacenter])


async def accepted_by_gate_a(tier: GateTier) -> None:
    assert await tier.coordinators["gate-a"].replicate_with_quorum(
        accepted_replica("gate-a"), tier.peers_of("gate-a"), QUORUM
    )


@pytest.mark.asyncio
async def test_a_deposed_leaders_revision_does_not_overwrite_its_successors_replica() -> None:
    tier = GateTier()
    await accepted_by_gate_a(tier)

    # Gate a is cut off; b takes the job over with c.
    tier.cut("gate-a", "gate-b", "gate-c")
    takeover = await tier.coordinators["gate-b"].take_over_committed_replica(
        JOB_ID, taken_over_by(tier, "gate-b"), tier.peers_of("gate-b"), QUORUM
    )
    # Healed, a -- still leading the job as far as it knows -- revises it.
    tier.heal()
    deposed_revision = await tier.coordinators["gate-a"].revise_committed_replica(
        JOB_ID, moved_to("dc-z"), tier.peers_of("gate-a"), QUORUM
    )

    assert takeover is not None
    assert deposed_revision is False
    for gate_id in ("gate-b", "gate-c"):
        assert tier.applied[gate_id].leader_id == "gate-b"
        assert tier.applied[gate_id].target_dcs == ["dc-a"]


@pytest.mark.asyncio
async def test_a_takeover_carries_the_old_leaders_last_revision() -> None:
    tier = GateTier()
    await accepted_by_gate_a(tier)

    # The revision reaches a quorum without b.
    tier.cut("gate-a", "gate-b")
    revised = await tier.coordinators["gate-a"].revise_committed_replica(
        JOB_ID, moved_to("dc-b"), tier.peers_of("gate-a"), QUORUM
    )
    # Gate a dies; b, which missed the revision, takes the job over.
    tier.heal()
    tier.cut("gate-a", "gate-b", "gate-c")
    takeover = await tier.coordinators["gate-b"].take_over_committed_replica(
        JOB_ID, taken_over_by(tier, "gate-b"), tier.peers_of("gate-b"), QUORUM
    )

    assert revised is True
    assert takeover is not None
    assert takeover.target_dcs == ["dc-b"]
    assert tier.applied["gate-c"] == takeover


@pytest.mark.asyncio
async def test_aborting_an_old_epochs_revision_leaves_the_new_epoch_standing() -> None:
    tier = GateTier()
    await accepted_by_gate_a(tier)
    tier.cut("gate-a", "gate-b", "gate-c")
    takeover = await tier.coordinators["gate-b"].take_over_committed_replica(
        JOB_ID, taken_over_by(tier, "gate-b"), tier.peers_of("gate-b"), QUORUM
    )

    # The deposed leader aborts a failed revision at the takeover's sequence.
    ack = GateJobReplicaAck.load(
        await tier.coordinators["gate-c"].handle_abort(
            GateJobReplicaAbort(
                job_id=JOB_ID,
                fence_token=1,
                sequence=takeover.sequence,
            ).dump()
        )
    )

    assert ack.status == GateJobReplicaStatus.ABORTED.value
    assert tier.coordinators["gate-c"].get_committed_replica(JOB_ID) == takeover
    assert tier.applied["gate-c"] == takeover


@pytest.mark.asyncio
async def test_a_takeover_no_quorum_answers_for_takes_nothing_over() -> None:
    tier = GateTier()
    await accepted_by_gate_a(tier)
    tier.cut("gate-b", "gate-a", "gate-c")

    takeover = await tier.coordinators["gate-b"].take_over_committed_replica(
        JOB_ID, taken_over_by(tier, "gate-b"), tier.peers_of("gate-b"), QUORUM
    )

    assert takeover is None
    assert tier.applied["gate-b"].leader_id == "gate-a"


@pytest.mark.asyncio
async def test_revisions_of_a_job_at_once_both_land() -> None:
    tier = GateTier()
    await accepted_by_gate_a(tier)
    coordinator = tier.coordinators["gate-a"]

    revisions = await asyncio.gather(
        coordinator.revise_committed_replica(
            JOB_ID,
            moved_to("dc-b"),
            tier.peers_of("gate-a"),
            QUORUM,
        ),
        coordinator.revise_committed_replica(
            JOB_ID,
            lambda committed: dataclasses.replace(committed, status_seed="completed"),
            tier.peers_of("gate-a"),
            QUORUM,
        ),
    )

    assert revisions == [True, True]
    for gate_id in GATE_ADDRESSES:
        assert (tier.applied[gate_id].target_dcs, tier.applied[gate_id].status_seed) == (
            ["dc-b"],
            "completed",
        )
