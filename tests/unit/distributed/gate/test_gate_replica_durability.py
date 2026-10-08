"""
A gate job replica survives power loss (A2-G-266).

The replica's two-phase commit kept prepared and committed replicas in
memory only: a gate that acknowledged a prepare and lost power forgot its
vote -- so it could vote for another job under the same idempotency key
(AD-40) -- and a power loss across the whole gate tier lost every replica,
fence token and key binding. Now every change to a job's 2PC state is
written to the gate's Raft store before the ack that depends on it, and a
restarted gate rebuilds its registries from that store.

Three real ``GateJobReplicationCoordinator`` instances -- gates a, b and c
-- each over a real ``RaftStore`` on its own ``SimFilesystem``, exchange
the protocol's messages in memory. A power loss keeps only what each disk
made durable; the gate restarts from it.
"""

import asyncio
import dataclasses
from pathlib import Path

import pytest

from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKey
from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.models import (
    GateJobReplica,
    GateJobReplicaAbort,
    GateJobReplicaCommit,
    GateJobReplicaPrepare,
    GateJobReplicaStatus,
)
from hyperscale.distributed.models.gate_replication import GateJobReplicaAck
from hyperscale.distributed.nodes.gate.replication_coordinator import (
    GATE_JOB_REPLICA_NAMESPACE,
    GateJobReplicationCoordinator,
)
from hyperscale.distributed.raft.store import RaftStore, RaftStoreCodec
from hyperscale.distributed.raft.store.models import KeyedStateRecord, KeyedStateReleasedRecord
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.taskex import TaskRunner
from tests.simulation.harness.sim import SeededRandom, SimFilesystem

from .test_gate_replica_versions import GATE_ADDRESSES, HANDLERS, JOB_ID, QUORUM, SilentLogger, accepted_replica

STORE_DIRECTORY = Path("/gate/data/raft")
KEY = IdempotencyKey(client_id="client-1", sequence=7, nonce="nonce-7")
GATE_IDS = tuple(GATE_ADDRESSES)


def keyed_replica(job_id: str, leader_id: str) -> GateJobReplica:
    return dataclasses.replace(accepted_replica(leader_id), job_id=job_id, idempotency_key=str(KEY))


class DurableGate:
    """One gate: its disk, the Raft store open on it, and its coordinator."""

    def __init__(self, tier: "DurableGateTier", gate_id: str, filesystem: SimFilesystem) -> None:
        self.tier = tier
        self.gate_id = gate_id
        self.filesystem = filesystem
        self.task_runner = TaskRunner()
        self.store = RaftStore(
            directory=STORE_DIRECTORY,
            filesystem=filesystem,
            random_source=SeededRandom(GATE_IDS.index(gate_id) + 1),
            clock=RealClock(),
            logger=SilentLogger(),
            task_runner=self.task_runner,
            set_aside_retained=1,
            storage_health=StorageHealth(),
        )
        self.applied: dict[str, GateJobReplica] = {}
        self.coordinator: GateJobReplicationCoordinator | None = None

    async def start(self) -> list[str]:
        """Open the store, build the coordinator and recover from the disk;
        the jobs it recovered a committed replica for."""
        recovery = await self.store.open(self.gate_id, is_this_node=lambda node_id_full: True)
        assert recovery.set_aside_reason is None, recovery.set_aside_reason
        self.coordinator = GateJobReplicationCoordinator(
            logger=SilentLogger(),
            task_runner=self.task_runner,
            get_node_id=lambda: type("NodeId", (), {"full": self.gate_id, "short": self.gate_id})(),
            get_node_addr=lambda: GATE_ADDRESSES[self.gate_id],
            send_tcp=self.tier.sender(self.gate_id),
            apply_committed=self._apply_committed,
            drop_committed=self._drop_committed,
            clock=RealClock(),
            storage=self.store,
        )
        recovered_job_ids = self.coordinator.recover_durable_replicas()
        await self.coordinator.apply_recovered_replicas(recovered_job_ids)
        return recovered_job_ids

    async def _apply_committed(self, replica: GateJobReplica) -> None:
        self.applied[replica.job_id] = replica

    async def _drop_committed(self, job_id: str) -> None:
        self.applied.pop(job_id, None)

    async def stop(self) -> None:
        await self.store.close()
        await self.task_runner.shutdown()


class DurableGateTier:
    """Three durable gates and the links between them."""

    def __init__(self) -> None:
        self.cut_links: set[frozenset[str]] = set()
        self.gates: dict[str, DurableGate] = {}

    async def start(self) -> "DurableGateTier":
        for gate_id in GATE_IDS:
            self.gates[gate_id] = DurableGate(self, gate_id, SimFilesystem())
            await self.gates[gate_id].start()
        return self

    def coordinator(self, gate_id: str) -> GateJobReplicationCoordinator:
        coordinator = self.gates[gate_id].coordinator
        assert coordinator is not None
        return coordinator

    def sender(self, gate_id: str):
        async def send_tcp(peer_address, action, payload, timeout):
            peer_id = next(peer for peer, address in GATE_ADDRESSES.items() if address == peer_address)
            if frozenset((gate_id, peer_id)) in self.cut_links:
                return ConnectionRefusedError(f"{peer_id} is unreachable"), 0
            handler = getattr(self.coordinator(peer_id), HANDLERS[action])
            return await handler(payload), 0

        return send_tcp

    def peers_of(self, gate_id: str) -> list[tuple[str, int]]:
        return [address for peer, address in GATE_ADDRESSES.items() if peer != gate_id]

    def cut(self, gate_id: str, *peer_ids: str) -> None:
        self.cut_links.update(frozenset((gate_id, peer_id)) for peer_id in peer_ids)

    async def power_loss(self, *gate_ids: str) -> dict[str, list[str]]:
        """Each gate loses power -- keeping only what its disk made durable
        -- and restarts from that disk; the jobs each recovered."""
        recovered: dict[str, list[str]] = {}
        for gate_id in gate_ids:
            gate = self.gates[gate_id]
            gate.filesystem.crash()
            durable = gate.filesystem.dump_durable()
            await gate.stop()
            restarted_filesystem = SimFilesystem()
            restarted_filesystem.restore_durable(durable)
            self.gates[gate_id] = DurableGate(self, gate_id, restarted_filesystem)
            recovered[gate_id] = await self.gates[gate_id].start()
        return recovered

    async def stop(self) -> None:
        for gate in self.gates.values():
            await gate.stop()


async def prepare_status(coordinator: GateJobReplicationCoordinator, replica: GateJobReplica) -> str:
    ack = GateJobReplicaAck.load(await coordinator.handle_prepare(GateJobReplicaPrepare(replica=replica).dump()))
    return ack.status


@pytest.mark.asyncio
async def test_a_committed_replica_survives_a_power_loss_of_every_gate() -> None:
    tier = await DurableGateTier().start()
    try:
        replica = keyed_replica(JOB_ID, "gate-a")
        assert await tier.coordinator("gate-a").replicate_with_quorum(replica, tier.peers_of("gate-a"), QUORUM)

        recovered = await tier.power_loss(*GATE_IDS)

        for gate_id in GATE_IDS:
            assert recovered[gate_id] == [JOB_ID], gate_id
            committed = tier.coordinator(gate_id).get_committed_replica(JOB_ID)
            assert committed == replica, gate_id
            assert (committed.fence_token, committed.leader_id) == (1, "gate-a")
            # Applied to the restarted gate's state, key binding included.
            assert tier.gates[gate_id].applied[JOB_ID].idempotency_key == str(KEY)
            # AD-40: no gate prepares another job under the key.
            assert await prepare_status(tier.coordinator(gate_id), keyed_replica("job-2", "gate-b")) == (
                GateJobReplicaStatus.REJECTED.value
            )
    finally:
        await tier.stop()


@pytest.mark.asyncio
async def test_a_prepare_vote_survives_the_voters_power_loss() -> None:
    tier = await DurableGateTier().start()
    try:
        first = keyed_replica("job-1", "gate-a")
        assert await prepare_status(tier.coordinator("gate-b"), first) == GateJobReplicaStatus.PREPARED.value

        await tier.power_loss("gate-b")

        # The vote gate b cast for job-1 holds the key: it does not vote
        # for job-2 under it.
        assert await prepare_status(tier.coordinator("gate-b"), keyed_replica("job-2", "gate-c")) == (
            GateJobReplicaStatus.REJECTED.value
        )
        assert tier.coordinator("gate-b").get_committed_replica("job-1") is None
    finally:
        await tier.stop()


@pytest.mark.asyncio
async def test_two_gates_cannot_admit_one_key_across_a_restart_of_the_overlap() -> None:
    tier = await DurableGateTier().start()
    try:
        # Gate a's prepare reaches b only; b then loses power.
        tier.cut("gate-a", "gate-c")
        assert await prepare_status(tier.coordinator("gate-b"), keyed_replica("job-1", "gate-a")) == (
            GateJobReplicaStatus.PREPARED.value
        )
        await tier.power_loss("gate-b")

        # Gate c admits job-2 under the same key: b (the quorum overlap)
        # still holds its vote, so c alone cannot reach a quorum.
        admitted = await tier.coordinator("gate-c").replicate_with_quorum(
            keyed_replica("job-2", "gate-c"), [GATE_ADDRESSES["gate-b"]], QUORUM
        )

        assert admitted is False
        assert tier.coordinator("gate-b").get_committed_replica("job-2") is None
    finally:
        await tier.stop()


@pytest.mark.asyncio
async def test_an_abort_after_a_restart_still_rolls_the_commit_back() -> None:
    tier = await DurableGateTier().start()
    try:
        replica = keyed_replica(JOB_ID, "gate-a")
        coordinator = tier.coordinator("gate-b")
        await coordinator.handle_commit(GateJobReplicaCommit(replica=replica).dump())
        await tier.power_loss("gate-b")
        assert tier.coordinator("gate-b").get_committed_replica(JOB_ID) == replica

        await tier.coordinator("gate-b").handle_abort(
            GateJobReplicaAbort(job_id=JOB_ID, fence_token=1, sequence=1).dump()
        )
        await tier.power_loss("gate-b")

        assert tier.coordinator("gate-b").get_committed_replica(JOB_ID) is None
        assert JOB_ID not in tier.gates["gate-b"].applied
    finally:
        await tier.stop()


@pytest.mark.asyncio
async def test_a_reaped_prepare_and_a_cleared_job_are_gone_after_a_restart() -> None:
    tier = await DurableGateTier().start()
    try:
        assert await tier.coordinator("gate-a").replicate_with_quorum(
            keyed_replica(JOB_ID, "gate-a"), tier.peers_of("gate-a"), QUORUM
        )
        coordinator = tier.coordinator("gate-b")
        assert await prepare_status(coordinator, keyed_replica("job-orphan", "gate-c")) == (
            GateJobReplicaStatus.REJECTED.value
        )
        unkeyed_orphan = dataclasses.replace(accepted_replica("gate-c"), job_id="job-orphan")
        assert await prepare_status(coordinator, unkeyed_orphan) == GateJobReplicaStatus.PREPARED.value
        coordinator._prepared_expires_at["job-orphan"] = 0.0
        assert await coordinator.reap_expired_prepared() == 1
        await coordinator.clear_for_job(JOB_ID)

        recovered = await tier.power_loss("gate-b")

        assert recovered["gate-b"] == []
        assert tier.coordinator("gate-b")._prepared == {}
        # The key is free again once its job is cleared.
        assert await prepare_status(tier.coordinator("gate-b"), keyed_replica("job-2", "gate-c")) == (
            GateJobReplicaStatus.PREPARED.value
        )
    finally:
        await tier.stop()


@pytest.mark.asyncio
async def test_a_revision_after_a_restart_never_reuses_a_sent_sequence() -> None:
    tier = await DurableGateTier().start()
    try:
        assert await tier.coordinator("gate-a").replicate_with_quorum(
            keyed_replica(JOB_ID, "gate-a"), tier.peers_of("gate-a"), QUORUM
        )
        # A revision that reached no quorum still sent sequence 2.
        tier.cut("gate-a", "gate-b", "gate-c")
        assert not await tier.coordinator("gate-a").revise_committed_replica(
            JOB_ID, lambda committed: dataclasses.replace(committed, target_dcs=["dc-b"]), tier.peers_of("gate-a"), QUORUM
        )
        await tier.power_loss("gate-a")
        tier.cut_links.clear()

        assert await tier.coordinator("gate-a").revise_committed_replica(
            JOB_ID, lambda committed: dataclasses.replace(committed, target_dcs=["dc-c"]), tier.peers_of("gate-a"), QUORUM
        )
        for gate_id in GATE_IDS:
            committed = tier.coordinator(gate_id).get_committed_replica(JOB_ID)
            assert (committed.sequence, committed.target_dcs) == (3, ["dc-c"]), gate_id
    finally:
        await tier.stop()


@pytest.mark.asyncio
async def test_the_store_keeps_each_keys_highest_version_whatever_order_writes_land_in() -> None:
    filesystem = SimFilesystem()
    gate = DurableGate(DurableGateTier(), "gate-a", filesystem)
    await gate.store.open("gate-a", is_this_node=lambda node_id_full: True)
    try:
        # Two writes of one key in flight together: the later version
        # reaches the disk first.
        await asyncio.gather(
            gate.store.write([KeyedStateRecord(namespace="ns", key="k", version=7, state=b"newer")]),
            gate.store.write([KeyedStateRecord(namespace="ns", key="k", version=6, state=b"older")]),
        )
        await gate.store.write(
            [
                KeyedStateRecord(namespace="ns", key="released", version=8, state=b"x"),
                KeyedStateReleasedRecord(namespace="ns", key="released", version=9),
                KeyedStateRecord(namespace="ns", key="released", version=5, state=b"stale"),
            ]
        )
        await gate.store.compact()
        filesystem.crash()
        durable = filesystem.dump_durable()
    finally:
        await gate.stop()

    restarted_filesystem = SimFilesystem()
    restarted_filesystem.restore_durable(durable)
    restarted = DurableGate(DurableGateTier(), "gate-a", restarted_filesystem)
    await restarted.store.open("gate-a", is_this_node=lambda node_id_full: True)
    try:
        recovered = restarted.store.take_recovered_states("ns")
        assert set(recovered) == {"k"}
        assert (recovered["k"].version, recovered["k"].state) == (7, b"newer")
        assert restarted.store.take_recovered_states("ns") == {}
        assert restarted.store.take_recovered_states(GATE_JOB_REPLICA_NAMESPACE) == {}
        _groups, states, _length = RaftStoreCodec().replay(
            restarted_filesystem.dump_durable()["files"][str(STORE_DIRECTORY / "store.wal")],
            restarted.store.identity.stamp,
        )
        # Compaction kept only the live state.
        assert list(states) == [("ns", "k")]
    finally:
        await restarted.stop()
