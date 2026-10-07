"""
Gate job-state replication coordinator (AD-31 takeover invariant).

Implements the two-phase commit protocol that makes a job's takeover
state durable across a quorum of live gates before the client sees
``JobAck(accepted=True)``. Backs the
``hyperscale.distributed.models.gate_replication`` capsule with the
prepared-state registry, peer fan-out, quorum collection, commit/abort
finalization, and idempotency tracking.

State on a gate is partitioned in two registries:

* ``_prepared`` — replicas this gate has acknowledged ``PREPARED`` for
  but not yet committed. Indexed by ``job_id`` with the most recent
  prepared sequence; older prepared sequences are evicted to keep the
  registry bounded. Prepared entries are **not** eligible for
  orphan-takeover (the user invariant: only committed replicas count).
* ``_committed_sequence`` — last committed sequence per ``job_id``.
  Used to make re-prepare and re-commit idempotent.

A replica's version is ``(fence_token, sequence)``: the fence is the
leadership epoch (a takeover raises it), the sequence orders one
leader's revisions within its epoch. A replica from an older epoch than
the one committed is ``REJECTED`` -- a deposed leader's revision must not
overwrite its successor's -- and one at or below the committed version
within the epoch is ``ALREADY_COMMITTED`` (a replay). Ordered by sequence
alone, a takeover whose sequence did not exceed a revision the old leader
committed was taken for a replay and never applied, and the old leader's
later revisions overwrote the new leader's.

A leader revises a job's replica one revision at a time
(``revise_committed_replica``), each built on the replica the one before
it committed, under a sequence no earlier attempt of its own used. A
takeover first adopts the freshest replica a quorum of gates committed
(``take_over_committed_replica``): built from a gate that missed the old
leader's last revision, it would have committed the older state over it.
* ``_committed_by_leader_addr`` — reverse index from leader TCP
  address to committed job ids. This lets the SWIM leader repair every
  committed job led by a dead gate without scanning unrelated jobs.

Committed replicas live in the gate's ``GateJobManager`` /
``GateRuntimeState`` (jobs, target dcs, callbacks, fence tokens,
workflow ids, submissions, leadership tracker) — this coordinator
applies them via callbacks supplied at construction so the storage
implementation stays with ``GateJobManager`` and not duplicated here.

The leader uses ``replicate_with_quorum`` as the entry point: it sends
prepare to every active peer, waits for ack count (with self) to meet
quorum, sends commit to prepared peers, and only returns success after
commit acks also meet quorum.

The peer side exposes ``handle_prepare``, ``handle_commit``,
``handle_abort``, ``handle_fetch`` — one per inbound RPC. These return
the wire ack the gate handlers serialize back over TCP.

Asyncio safety: all mutation paths go through a single
``asyncio.Lock`` so concurrent prepares for the same job from
overlapping submissions cannot race the prepared/committed registries.

Durability (A2-G-266): every change to a job's prepared, committed or
rollback state is written to the node's Raft store -- the identity-stamped,
group-committed store that keeps its Raft groups (D1) -- as the job's whole
state, and no ack, commit or abort that depends on it answers before that
write is durable. A gate that acked a prepare and crashed would otherwise
forget its vote (two gates could then each count it for a different job
under one idempotency key), and a whole-tier restart would lose every
replica. The state is versioned, not locked across the write: writes of
different jobs group-commit together, and the store keeps each job's
highest version, so one that reaches the disk late never overwrites a newer
one. ``recover_durable_replicas`` rebuilds the registries at start, before
the gate answers any replica RPC.

Why not the job's Raft group: an idempotency key binds one job across
jobs (AD-40), which per-job groups cannot decide -- only quorum
intersection of these prepares can; and the committed replica is what
founds the job's group (its voters), so the group cannot carry it.
"""

import asyncio
import dataclasses
from typing import TYPE_CHECKING, Awaitable, Callable

import msgspec

from hyperscale.distributed.models import (
    GateJobReplica,
    GateJobReplicaAbort,
    GateJobReplicaAck,
    GateJobReplicaCommit,
    GateJobReplicaFetchRequest,
    GateJobReplicaFetchResponse,
    GateJobReplicaPrepare,
    GateJobReplicaStatus,
)
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerWarning,
)

from hyperscale.distributed.raft.store.models import KeyedStateRecord, KeyedStateReleasedRecord
from hyperscale.distributed.raft.store.raft_storage import RaftStorage
from hyperscale.distributed.runtime import Clock

from .models import GateJobReplicaDurableState, GateJobReplicaRollback

# The namespace of a gate's job replica states in its Raft store.
GATE_JOB_REPLICA_NAMESPACE = "gate_job_replica"


if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.taskex import TaskRunner


class GateJobReplicationCoordinator:
    """Two-phase commit coordinator for gate job-state replication."""

    __slots__ = (
        "_clock",
        "_logger",
        "_task_runner",
        "_get_node_id",
        "_get_node_addr",
        "_send_tcp",
        "_apply_committed",
        "_drop_committed",
        "_lock",
        "_prepared",
        "_committed_sequence",
        "_committed_replicas",
        "_committed_by_leader_addr",
        "_commit_rollback_replicas",
        "_commit_rollback_expires_at",
        "_prepared_expires_at",
        "_prepared_ttl_seconds",
        "_quorum_timeout_seconds",
        "_peer_rpc_timeout_seconds",
        "_attempted_sequences",
        "_revision_locks",
        "_storage",
        "_durable_version",
        "_state_encoder",
        "_state_decoder",
    )

    def __init__(
        self,
        logger: Logger,
        task_runner: "TaskRunner",
        get_node_id: Callable[[], "NodeId"],
        get_node_addr: Callable[[], tuple[str, int]],
        send_tcp: Callable[
            [tuple[str, int], str, bytes, float], Awaitable[bytes]
        ],
        apply_committed: Callable[[GateJobReplica], Awaitable[None]],
        drop_committed: Callable[[str], Awaitable[None]],
        clock: Clock,
        storage: RaftStorage,
        prepared_ttl_seconds: float = 30.0,
        quorum_timeout_seconds: float = 5.0,
        peer_rpc_timeout_seconds: float = 5.0,
    ) -> None:
        self._clock: Clock = clock
        self._logger = logger
        self._task_runner = task_runner
        self._get_node_id = get_node_id
        self._get_node_addr = get_node_addr
        self._send_tcp = send_tcp
        self._apply_committed = apply_committed
        self._drop_committed = drop_committed
        self._lock = asyncio.Lock()
        self._prepared: dict[str, GateJobReplica] = {}
        self._committed_sequence: dict[str, int] = {}
        self._committed_replicas: dict[str, GateJobReplica] = {}
        self._committed_by_leader_addr: dict[tuple[str, int], set[str]] = {}
        # job_id -> (fence_token, sequence) of a commit -> the replica it
        # replaced, restored if the commit is aborted.
        self._commit_rollback_replicas: dict[
            str,
            dict[tuple[int, int], GateJobReplica | None],
        ] = {}
        self._commit_rollback_expires_at: dict[tuple[str, int, int], float] = {}
        self._prepared_expires_at: dict[str, float] = {}
        self._prepared_ttl_seconds = prepared_ttl_seconds
        self._quorum_timeout_seconds = quorum_timeout_seconds
        self._peer_rpc_timeout_seconds = peer_rpc_timeout_seconds
        # The highest sequence this gate sent out for each job, committed
        # or not: a revision never reuses one.
        self._attempted_sequences: dict[str, int] = {}
        # One revision (or takeover) of a job's replica at a time.
        self._revision_locks: dict[str, asyncio.Lock] = {}
        # Where each job's state is kept (the node's Raft store), and the
        # version its next write takes -- above every version on disk.
        self._storage = storage
        self._durable_version = 0
        self._state_encoder = msgspec.msgpack.Encoder()
        self._state_decoder = msgspec.msgpack.Decoder(GateJobReplicaDurableState)

    # ------------------------------------------------------------------
    # Leader-side entry point
    # ------------------------------------------------------------------

    async def replicate_with_quorum(
        self,
        replica: GateJobReplica,
        peer_addrs: list[tuple[str, int]],
        quorum_size: int,
    ) -> bool:
        """Run prepare → commit (or abort) across peers, return success.

        ``peer_addrs`` is every active peer gate's TCP address. ``self``
        is *not* in this list — the leader counts itself toward
        quorum automatically.

        Returns ``True`` when the replica is committed locally and at
        least ``quorum_size - 1`` peer commit-acks were collected after
        prepare quorum. Returns ``False`` when quorum could not be
        reached; on a ``False`` return the leader has already attempted
        to abort every prepared peer and reject the client submission.

        ``quorum_size`` is the cluster-wide majority threshold (e.g.
        2 for a 3-gate cluster). A single-gate cluster passes
        ``quorum_size=1`` and an empty ``peer_addrs`` list — local
        commit alone satisfies quorum and no network round-trips are
        performed.
        """
        peer_acks_needed = max(0, quorum_size - 1)
        self._attempted_sequences[replica.job_id] = max(
            replica.sequence,
            self._attempted_sequences.get(replica.job_id, 0),
        )

        # The leader's own vote is a prepare like any peer's: a replica its
        # registry refuses (an older epoch, or another job holding the
        # idempotency key) is not counted -- it was, unchecked, so two
        # gates admitting one key at once each counted itself and
        # committed.
        if await self._record_prepare(replica) is GateJobReplicaStatus.REJECTED:
            return False

        return await self._replicate_prepared(replica, peer_addrs, peer_acks_needed)

    async def _replicate_prepared(
        self,
        replica: GateJobReplica,
        peer_addrs: list[tuple[str, int]],
        peer_acks_needed: int,
    ) -> bool:
        """With the leader's own prepare recorded: commit at once when no
        peer ack is needed, refuse when too few peers exist, else run the
        two-phase commit across the peers."""
        if peer_acks_needed == 0:
            await self._apply_committed_with_tracking(replica)
            return True

        if len(peer_addrs) < peer_acks_needed:
            await self._drop_own_prepare(replica)
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Gate replication: insufficient peers for quorum "
                        f"job={replica.job_id[:10]} need={peer_acks_needed} "
                        f"available={len(peer_addrs)}"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                )
            )
            return False

        return await self._prepare_on_peers(replica, peer_addrs, peer_acks_needed)

    async def _drop_own_prepare(self, replica: GateJobReplica) -> None:
        """Drop the leader's own prepare of this replica, unless another
        prepare replaced it."""
        async with self._lock:
            if self._prepared.get(replica.job_id) is not replica:
                return
            self._prepared.pop(replica.job_id, None)
            self._prepared_expires_at.pop(replica.job_id, None)
            record = self._durable_record_locked(replica.job_id)
        await self._storage.write([record])

    async def _prepare_on_peers(
        self,
        replica: GateJobReplica,
        peer_addrs: list[tuple[str, int]],
        peer_acks_needed: int,
    ) -> bool:
        """Phase one: prepare the replica on every peer; abort what was
        prepared when too few acked, else go on to commit."""
        prepare_payload = GateJobReplicaPrepare(replica=replica).dump()
        ack_results = await asyncio.gather(
            *[
                self._send_prepare(peer_addr, prepare_payload, replica.job_id)
                for peer_addr in peer_addrs
            ],
            return_exceptions=True,
        )

        acked_peers = self._prepare_acked_peers(peer_addrs, ack_results)

        if len(acked_peers) < peer_acks_needed:
            await self._drop_own_prepare(replica)
            await self._abort_prepared_peers(acked_peers, replica)
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Gate replication: quorum unavailable "
                        f"job={replica.job_id[:10]} prepared={len(acked_peers)} "
                        f"needed={peer_acks_needed}"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                )
            )
            return False

        return await self._commit_on_peers(replica, acked_peers, peer_acks_needed)

    def _prepare_acked_peers(
        self,
        peer_addrs: list[tuple[str, int]],
        ack_results: list[GateJobReplicaAck | BaseException | None],
    ) -> list[tuple[str, int]]:
        """The peers whose prepare ack was positive."""
        return [
            peer_addr
            for peer_addr, ack_result in zip(peer_addrs, ack_results)
            if self._is_prepare_ack_positive(ack_result)
        ]

    async def _commit_on_peers(
        self,
        replica: GateJobReplica,
        acked_peers: list[tuple[str, int]],
        peer_acks_needed: int,
    ) -> bool:
        """Phase two: commit the replica on the prepared peers; abort when
        too few committed, else commit locally."""
        commit_payload = GateJobReplicaCommit(replica=replica).dump()
        commit_results = await asyncio.gather(
            *[
                self._send_commit(peer_addr, commit_payload, replica.job_id)
                for peer_addr in acked_peers
            ],
            return_exceptions=True,
        )

        committed_peers = self._commit_acked_peers(acked_peers, commit_results)
        if len(committed_peers) < peer_acks_needed:
            await self._drop_own_prepare(replica)
            await self._abort_prepared_peers(acked_peers, replica)
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Gate replication: commit quorum unavailable "
                        f"job={replica.job_id[:10]} committed={len(committed_peers)} "
                        f"needed={peer_acks_needed}"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                )
            )
            return False

        return await self._commit_locally(replica, acked_peers)

    def _commit_acked_peers(
        self,
        acked_peers: list[tuple[str, int]],
        commit_results: list[GateJobReplicaAck | BaseException | None],
    ) -> list[tuple[str, int]]:
        """The prepared peers whose commit ack was positive."""
        return [
            peer_addr
            for peer_addr, commit_result in zip(acked_peers, commit_results)
            if self._is_commit_ack_positive(commit_result)
        ]

    async def _commit_locally(
        self,
        replica: GateJobReplica,
        acked_peers: list[tuple[str, int]],
    ) -> bool:
        """Commit the quorum-committed replica here; a local failure aborts
        it on the prepared peers."""
        try:
            await self._apply_committed_with_tracking(replica)
        except Exception as apply_error:
            await self._abort_prepared_peers(acked_peers, replica)
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Gate replication: local commit failed "
                        f"job={replica.job_id[:10]}: {apply_error}"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                )
            )
            return False

        return True

    async def revise_committed_replica(
        self,
        job_id: str,
        revise: Callable[[GateJobReplica], GateJobReplica | None],
        peer_addrs: list[tuple[str, int]],
        quorum_size: int,
    ) -> bool:
        """Commit a revision of ``job_id``'s replica -- its leader's own
        change, in its own epoch -- to a quorum of gates.

        ``revise`` turns the committed replica into its successor, or
        returns None when there is nothing to change (True: nothing to
        commit). Revisions of a job run one at a time, each built on the
        replica the one before it committed, under a sequence no earlier
        attempt of this gate used: two built on the same replica at once,
        and the later commit dropped the earlier one's change; a sequence
        that a failed attempt left committed at a peer was answered
        "already committed" there for a different replica.

        False when this gate holds no committed replica of the job, or no
        quorum committed the revision.
        """
        async with self._revision_locks.setdefault(job_id, asyncio.Lock()):
            committed = self._committed_replicas.get(job_id)
            if committed is None:
                return False
            if (revised := revise(committed)) is None:
                return True
            return await self.replicate_with_quorum(
                dataclasses.replace(
                    revised,
                    fence_token=committed.fence_token,
                    sequence=max(
                        committed.sequence,
                        self._attempted_sequences.get(job_id, 0),
                    )
                    + 1,
                ),
                peer_addrs,
                quorum_size,
            )

    async def take_over_committed_replica(
        self,
        job_id: str,
        build_takeover: Callable[[], GateJobReplica | None],
        peer_addrs: list[tuple[str, int]],
        quorum_size: int,
    ) -> GateJobReplica | None:
        """Commit this gate's takeover of ``job_id`` to a quorum of gates.

        The freshest replica a quorum committed is adopted first -- every
        revision of the old leader reached a quorum, so one of them holds
        its last -- and applied here, where ``build_takeover`` reads the
        job's state to build the takeover replica (a raised fence; None
        to give up). Built from a gate that missed the old leader's last
        revision, the takeover committed the older state over it.

        None when a quorum did not answer for the job, ``build_takeover``
        gave up, or no quorum committed the takeover.
        """
        async with self._revision_locks.setdefault(job_id, asyncio.Lock()):
            if not await self._adopt_quorum_freshest_replica(job_id, peer_addrs, quorum_size):
                return None

            return await self._commit_takeover(job_id, build_takeover, peer_addrs, quorum_size)

    async def _adopt_quorum_freshest_replica(
        self,
        job_id: str,
        peer_addrs: list[tuple[str, int]],
        quorum_size: int,
    ) -> bool:
        """Adopt the freshest replica a quorum (this gate among it) answers
        with, when newer than this gate's; False when no quorum answered."""
        responses = await self._fetch_job_replica_responses(job_id, peer_addrs)
        answers = self._fetch_answers(responses)
        if len(answers) + 1 < quorum_size:
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Gate takeover of job {job_id[:10]}: {len(answers)} of "
                        f"{len(peer_addrs)} peers answered for its replica, short "
                        f"of a quorum of {quorum_size} with this gate"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                )
            )
            return False

        await self._adopt_if_fresher(job_id, self._freshest_answered_replica(answers))
        return True

    @staticmethod
    def _fetch_answers(
        responses: list[GateJobReplicaFetchResponse | BaseException | None],
    ) -> list[GateJobReplicaFetchResponse]:
        """The peers' answers that are fetch responses."""
        return [
            response
            for response in responses
            if isinstance(response, GateJobReplicaFetchResponse)
        ]

    def _freshest_answered_replica(
        self,
        answers: list[GateJobReplicaFetchResponse],
    ) -> GateJobReplica | None:
        """The freshest replica among the answers that hold one."""
        return max(
            (
                answer.replica
                for answer in answers
                if self._answer_holds_replica(answer)
            ),
            key=self._replica_version,
            default=None,
        )

    async def _adopt_if_fresher(self, job_id: str, freshest: GateJobReplica | None) -> None:
        """Apply the freshest answered replica when it is newer than the one
        committed here."""
        local = self._committed_replicas.get(job_id)
        if freshest is not None and self._is_fresher(freshest, local):
            await self._apply_repair_replica(freshest)

    async def _commit_takeover(
        self,
        job_id: str,
        build_takeover: Callable[[], GateJobReplica | None],
        peer_addrs: list[tuple[str, int]],
        quorum_size: int,
    ) -> GateJobReplica | None:
        """Build the takeover replica under a sequence no earlier attempt
        used and commit it to a quorum; None when either fails."""
        if (takeover := build_takeover()) is None:
            return None
        takeover = dataclasses.replace(
            takeover,
            sequence=self._next_takeover_sequence(job_id),
        )
        if not await self.replicate_with_quorum(takeover, peer_addrs, quorum_size):
            return None
        return takeover

    def _next_takeover_sequence(self, job_id: str) -> int:
        """One past both the committed sequence and any this gate attempted."""
        committed = self._committed_replicas.get(job_id)
        return (
            max(
                committed.sequence if committed is not None else 0,
                self._attempted_sequences.get(job_id, 0),
            )
            + 1
        )

    # ------------------------------------------------------------------
    # Peer-side handlers (entry points for the TCP wire handlers)
    # ------------------------------------------------------------------

    async def handle_prepare(self, data: bytes) -> bytes:
        """Apply ``data`` to the prepared registry; return wire ack."""
        prepare = GateJobReplicaPrepare.load(data)
        replica = prepare.replica
        status = await self._record_prepare(replica)
        return GateJobReplicaAck(
            job_id=replica.job_id,
            sequence=replica.sequence,
            status=status.value,
            responder_id=self._get_node_id().full,
        ).dump()

    async def handle_commit(self, data: bytes) -> bytes:
        """Promote ``(job_id, sequence)`` from prepared to committed."""
        commit = GateJobReplicaCommit.load(data)
        replica = commit.replica
        status = await self._apply_commit(replica)
        return GateJobReplicaAck(
            job_id=replica.job_id,
            sequence=replica.sequence,
            status=status.value,
            responder_id=self._get_node_id().full,
        ).dump()

    async def handle_abort(self, data: bytes) -> bytes:
        """Drop prepared state for ``(job_id, sequence)``."""
        abort = GateJobReplicaAbort.load(data)
        status = await self._drop_prepared_or_committed(
            abort.job_id,
            abort.fence_token,
            abort.sequence,
        )
        return GateJobReplicaAck(
            job_id=abort.job_id,
            sequence=abort.sequence,
            status=status.value,
            responder_id=self._get_node_id().full,
        ).dump()

    async def handle_fetch(self, data: bytes) -> bytes:
        """Return cached committed replicas for state repair."""
        request = GateJobReplicaFetchRequest.load(data)
        if request.job_id is not None:
            return await self._answer_job_fetch(request.job_id)

        if request.leader_addr is None:
            return GateJobReplicaFetchResponse(found=False).dump()

        async with self._lock:
            replicas = self._get_committed_replicas_for_leader_locked(
                request.leader_addr,
            )
        return GateJobReplicaFetchResponse(
            leader_addr=request.leader_addr,
            replicas=replicas,
            found=bool(replicas),
        ).dump()

    async def _answer_job_fetch(self, job_id: str) -> bytes:
        """Answer a fetch for one job with its committed replica, if any."""
        async with self._lock:
            replica = self._committed_replicas.get(job_id)
        return GateJobReplicaFetchResponse(
            job_id=job_id,
            replica=replica,
            replicas=[replica] if replica is not None else [],
            found=replica is not None,
        ).dump()

    # ------------------------------------------------------------------
    # Orphan-recovery state-repair helper (Phase 2)
    # ------------------------------------------------------------------

    async def fetch_committed_replica_from_peers(
        self,
        job_id: str,
        peer_addrs: list[tuple[str, int]],
    ) -> GateJobReplica | None:
        """Query peers for a cached committed replica for ``job_id``.

        Returns the freshest replica any peer holds -- its leader revises
        a job's replica, and each peer holds the latest revision it
        committed. Returns ``None`` when no peer has a committed copy —
        typically meaning the job's quorum commit was lost with the
        original leader or never reached any survivor.
        """
        if not peer_addrs:
            return None

        responses = await self._fetch_job_replica_responses(job_id, peer_addrs)

        # Each peer holds the latest revision it committed: the freshest
        # answer is the job's state.
        return self._freshest_fetched_replica(responses)

    def _freshest_fetched_replica(
        self,
        responses: list[GateJobReplicaFetchResponse | BaseException | None],
    ) -> GateJobReplica | None:
        """The freshest replica among the peers' answers that hold one."""
        return max(
            (
                response.replica
                for response in responses
                if self._is_replica_answer(response)
            ),
            key=self._replica_version,
            default=None,
        )

    async def _fetch_job_replica_responses(
        self,
        job_id: str,
        peer_addrs: list[tuple[str, int]],
    ) -> list[GateJobReplicaFetchResponse | BaseException | None]:
        """Ask every peer for its committed replica of the job."""
        request_payload = GateJobReplicaFetchRequest(job_id=job_id).dump()
        return await asyncio.gather(
            *[
                self._send_fetch(
                    peer_addr,
                    request_payload,
                    expected_job_id=job_id,
                    expected_leader_addr=None,
                )
                for peer_addr in peer_addrs
            ],
            return_exceptions=True,
        )

    def _is_replica_answer(self, response: object) -> bool:
        """Whether a peer's answer is a fetch response holding a replica."""
        return isinstance(response, GateJobReplicaFetchResponse) and self._answer_holds_replica(response)

    @staticmethod
    def _answer_holds_replica(answer: GateJobReplicaFetchResponse) -> bool:
        """Whether a fetch response found a committed replica."""
        return answer.found and answer.replica is not None

    async def fetch_committed_replicas_for_leader_from_peers(
        self,
        leader_addr: tuple[str, int],
        peer_addrs: list[tuple[str, int]],
    ) -> list[GateJobReplica]:
        """Fetch committed replicas whose current leader is ``leader_addr``."""
        if not peer_addrs:
            return []

        responses = await self._fetch_leader_replica_responses(leader_addr, peer_addrs)

        replicas_by_job_id: dict[str, GateJobReplica] = {}
        for response in responses:
            self._merge_fetched_replicas(replicas_by_job_id, response)

        return list(replicas_by_job_id.values())

    async def _fetch_leader_replica_responses(
        self,
        leader_addr: tuple[str, int],
        peer_addrs: list[tuple[str, int]],
    ) -> list[GateJobReplicaFetchResponse | BaseException | None]:
        """Ask every peer for its committed replicas led by ``leader_addr``."""
        request_payload = GateJobReplicaFetchRequest(
            leader_addr=leader_addr,
        ).dump()
        return await asyncio.gather(
            *[
                self._send_fetch(
                    peer_addr,
                    request_payload,
                    expected_job_id=None,
                    expected_leader_addr=leader_addr,
                )
                for peer_addr in peer_addrs
            ],
            return_exceptions=True,
        )

    def _merge_fetched_replicas(
        self,
        replicas_by_job_id: dict[str, GateJobReplica],
        response: GateJobReplicaFetchResponse | BaseException | None,
    ) -> None:
        """Keep the freshest of a peer's answered replicas per job; a failed
        or empty answer adds none."""
        if isinstance(response, Exception) or response is None:
            return
        self._keep_freshest_replicas(replicas_by_job_id, response.replicas)

    def _keep_freshest_replicas(
        self,
        replicas_by_job_id: dict[str, GateJobReplica],
        replicas: list[GateJobReplica],
    ) -> None:
        """Keep, per job, the replica at the highest (fence token, sequence)."""
        for replica in replicas:
            current = replicas_by_job_id.get(replica.job_id)
            if self._is_fresher(replica, current):
                replicas_by_job_id[replica.job_id] = replica

    @staticmethod
    def _replica_version(replica: GateJobReplica) -> tuple[int, int]:
        """A replica's version: its (fence token, sequence)."""
        return (replica.fence_token, replica.sequence)

    @staticmethod
    def _is_fresher(replica: GateJobReplica, current: GateJobReplica | None) -> bool:
        """Whether there is no current replica, or ``replica`` is newer."""
        return current is None or (replica.fence_token, replica.sequence) > (
            current.fence_token,
            current.sequence,
        )

    async def repair_committed_replica_from_peers(
        self,
        job_id: str,
        peer_addrs: list[tuple[str, int]],
    ) -> bool:
        """Fetch and apply a committed replica for ``job_id`` from peers."""
        async with self._lock:
            local_replica = self._committed_replicas.get(job_id)
        if local_replica is not None:
            await self._apply_committed(local_replica)
            return True

        replica = await self.fetch_committed_replica_from_peers(
            job_id,
            peer_addrs,
        )
        if replica is None:
            return False
        return await self._apply_repair_replica(replica)

    async def repair_committed_replicas_for_leader_from_peers(
        self,
        leader_addr: tuple[str, int],
        peer_addrs: list[tuple[str, int]],
    ) -> list[str]:
        """Fetch and apply committed replicas led by ``leader_addr``."""
        async with self._lock:
            local_replicas = self._get_committed_replicas_for_leader_locked(
                leader_addr,
            )
        replicas = await self.fetch_committed_replicas_for_leader_from_peers(
            leader_addr,
            peer_addrs,
        )
        replicas_by_job_id = {
            replica.job_id: replica
            for replica in local_replicas
        }
        self._keep_freshest_replicas(replicas_by_job_id, replicas)

        return await self._apply_repair_replicas(list(replicas_by_job_id.values()))

    async def _apply_repair_replicas(self, replicas: list[GateJobReplica]) -> list[str]:
        """Apply each repair replica; returns the jobs repaired."""
        repaired_job_ids: list[str] = []
        for replica in replicas:
            repaired = await self._apply_repair_replica(replica)
            if repaired:
                repaired_job_ids.append(replica.job_id)
        return repaired_job_ids

    # ------------------------------------------------------------------
    # Internal state mutations (all under self._lock)
    # ------------------------------------------------------------------

    async def _record_prepare(
        self, replica: GateJobReplica
    ) -> GateJobReplicaStatus:
        """Apply a prepare to the in-memory registry.

        Returns:
            ``REJECTED`` when the replica is from an older epoch than
            the one committed, or a strictly newer prepare is already
            in flight for the same job.
            ``ALREADY_COMMITTED`` when the committed version is at or
            above the replica's (idempotent replay).
            ``PREPARED`` when the replica's version is newer than the
            committed one or no commit exists yet.
        """
        async with self._lock:
            if (status := self._prepare_status_locked(replica)) is GateJobReplicaStatus.REJECTED:
                return status
            # A vote -- or a commit it answers for -- is durable before
            # it is counted.
            record = self._durable_record_locked(replica.job_id)

        await self._storage.write([record])
        return status

    def _prepare_status_locked(self, replica: GateJobReplica) -> GateJobReplicaStatus:
        """Under the lock: the committed replica's answer to the prepare,
        else REJECTED when it is refused, else PREPARED -- held."""
        if (verdict := self._committed_verdict_locked(replica)) is not None:
            return verdict
        if self._prepare_refused_locked(replica):
            return GateJobReplicaStatus.REJECTED
        return self._hold_prepare_locked(replica)

    def _hold_prepare_locked(self, replica: GateJobReplica) -> GateJobReplicaStatus:
        """Under the lock: hold the replica as this gate's prepare of its job."""
        self._prepared[replica.job_id] = replica
        self._prepared_expires_at[replica.job_id] = (
            self._clock.monotonic() + self._prepared_ttl_seconds
        )
        return GateJobReplicaStatus.PREPARED

    def _committed_verdict_locked(self, replica: GateJobReplica) -> GateJobReplicaStatus | None:
        """Under the lock: the answer the job's committed replica gives a
        prepare or commit of ``replica``; None when it allows it."""
        if (committed := self._committed_replicas.get(replica.job_id)) is None:
            return None
        return self._verdict_against_committed(replica, committed)

    @staticmethod
    def _verdict_against_committed(
        replica: GateJobReplica,
        committed: GateJobReplica,
    ) -> GateJobReplicaStatus | None:
        """REJECTED for an older epoch, ALREADY_COMMITTED for a version at
        or below the committed one, else None."""
        if replica.fence_token < committed.fence_token:
            return GateJobReplicaStatus.REJECTED
        return (
            GateJobReplicaStatus.ALREADY_COMMITTED
            if (committed.fence_token, committed.sequence) >= (replica.fence_token, replica.sequence)
            else None
        )

    def _prepare_refused_locked(self, replica: GateJobReplica) -> bool:
        """Under the lock: whether a newer prepare of the job is in flight,
        or another job holds the replica's idempotency key."""
        return self._newer_prepare_in_flight_locked(replica) or self._idempotency_key_held_elsewhere_locked(replica)

    def _newer_prepare_in_flight_locked(self, replica: GateJobReplica) -> bool:
        """Under the lock: whether a strictly newer prepare of the job is held."""
        existing = self._prepared.get(replica.job_id)
        return existing is not None and (existing.fence_token, existing.sequence) > (
            replica.fence_token,
            replica.sequence,
        )

    def _idempotency_key_held_elsewhere_locked(self, replica: GateJobReplica) -> bool:
        """Under the lock: whether another job's replica holds the key."""
        # AD-40: an idempotency key decides one job. A key another job's
        # replica holds here -- prepared or committed -- refuses this
        # one: quorums intersect, so of two gates admitting the same
        # key at once, at most one commits.
        return bool(replica.idempotency_key) and any(
            self._holds_key_of_another_job(held, replica)
            for held in (*self._committed_replicas.values(), *self._prepared.values())
        )

    @staticmethod
    def _holds_key_of_another_job(held: GateJobReplica, replica: GateJobReplica) -> bool:
        """Whether ``held`` is another job's replica under the same key."""
        return held.idempotency_key == replica.idempotency_key and held.job_id != replica.job_id

    async def _apply_commit(
        self, replica: GateJobReplica
    ) -> GateJobReplicaStatus:
        """Promote prepared → committed, or commit directly if no prepare.

        A peer can receive ``commit`` without having seen the matching
        ``prepare`` (network drop or restart between the two). The
        commit message carries the full replica so the peer can apply
        it directly in that case, equivalent to ``prepare`` followed
        immediately by ``commit``.
        """
        async with self._lock:
            if (status := self._commit_status_locked(replica)) is GateJobReplicaStatus.REJECTED:
                return status
            # ALREADY_COMMITTED answers for a commit whose own write may
            # still be in flight: it is written again, so the answer
            # waits on it being durable.
            record = self._durable_record_locked(replica.job_id)

        await self._storage.write([record])
        if status is GateJobReplicaStatus.COMMITTED:
            await self._apply_committed(replica)
        return status

    def _commit_status_locked(self, replica: GateJobReplica) -> GateJobReplicaStatus:
        """Under the lock: the committed replica's answer to the commit,
        else COMMITTED -- recorded, its rollback kept."""
        if (verdict := self._committed_verdict_locked(replica)) is not None:
            return verdict
        self._prepared.pop(replica.job_id, None)
        self._prepared_expires_at.pop(replica.job_id, None)
        self._record_committed_locked(replica, track_rollback=True)
        return GateJobReplicaStatus.COMMITTED

    async def _drop_prepared_or_committed(
        self, job_id: str, fence_token: int, sequence: int
    ) -> GateJobReplicaStatus:
        """Drop the prepared or committed replica of exactly this version:
        an abort of one epoch's revision must not take down another
        epoch's at the same sequence."""
        async with self._lock:
            self._drop_prepared_version_locked(job_id, fence_token, sequence)
            restore_replica, drop_committed = self._roll_back_committed_locked(
                job_id, fence_token, sequence
            )
            record = self._durable_record_locked(job_id)

        await self._storage.write([record])
        await self._publish_rollback(job_id, restore_replica, drop_committed)

        return GateJobReplicaStatus.ABORTED

    def _drop_prepared_version_locked(self, job_id: str, fence_token: int, sequence: int) -> None:
        """Under the lock: drop the job's prepare when it is of exactly this version."""
        if self._is_version(self._prepared.get(job_id), fence_token, sequence):
            self._prepared.pop(job_id, None)
            self._prepared_expires_at.pop(job_id, None)

    @staticmethod
    def _is_version(replica: GateJobReplica | None, fence_token: int, sequence: int) -> bool:
        """Whether the replica exists at exactly this (fence token, sequence)."""
        return replica is not None and (replica.fence_token, replica.sequence) == (
            fence_token,
            sequence,
        )

    def _roll_back_committed_locked(
        self,
        job_id: str,
        fence_token: int,
        sequence: int,
    ) -> tuple[GateJobReplica | None, bool]:
        """Under the lock: roll the commit of exactly this version back to
        the replica it replaced; returns the replica restored, and whether
        the job's commit was dropped with none to restore."""
        if not self._is_version(self._committed_replicas.get(job_id), fence_token, sequence):
            return None, False

        job_rollbacks = self._commit_rollback_replicas.get(job_id, {})
        has_rollback_record = (fence_token, sequence) in job_rollbacks
        previous_replica = job_rollbacks.pop((fence_token, sequence), None)
        if not job_rollbacks:
            self._commit_rollback_replicas.pop(job_id, None)
        self._commit_rollback_expires_at.pop((job_id, fence_token, sequence), None)
        return self._restore_previous_commit_locked(job_id, has_rollback_record, previous_replica)

    def _restore_previous_commit_locked(
        self,
        job_id: str,
        has_rollback_record: bool,
        previous_replica: GateJobReplica | None,
    ) -> tuple[GateJobReplica | None, bool]:
        """Under the lock: replace a rolled-back commit with the replica it
        replaced, or drop it when it replaced none."""
        if not has_rollback_record:
            return None, False

        self._drop_committed_locked(job_id)
        if previous_replica is None:
            return None, True

        self._record_committed_locked(
            previous_replica,
            track_rollback=False,
        )
        return previous_replica, False

    async def _publish_rollback(
        self,
        job_id: str,
        restore_replica: GateJobReplica | None,
        drop_committed: bool,
    ) -> None:
        """Apply a rolled-back commit's restored replica, or drop the job's
        commit when none was restored."""
        if restore_replica is not None:
            await self._apply_committed(restore_replica)
        elif drop_committed:
            await self._drop_committed(job_id)

    async def _apply_repair_replica(self, replica: GateJobReplica) -> bool:
        """Apply a committed replica from the repair path."""
        async with self._lock:
            if not self._record_repair_locked(replica):
                return False
            record = self._durable_record_locked(replica.job_id)

        await self._storage.write([record])
        await self._apply_committed(replica)
        return True

    def _record_repair_locked(self, replica: GateJobReplica) -> bool:
        """Under the lock: record a repair replica no older than the one
        committed here; False when the committed one is newer."""
        committed_version = self._committed_version_locked(replica.job_id)
        replica_version = (replica.fence_token, replica.sequence)
        if self._is_newer_version(committed_version, replica_version):
            return False
        if committed_version != replica_version:
            self._record_committed_locked(replica, track_rollback=False)
        return True

    def _committed_version_locked(self, job_id: str) -> tuple[int, int] | None:
        """Under the lock: the (fence token, sequence) committed for the job."""
        committed = self._committed_replicas.get(job_id)
        return (committed.fence_token, committed.sequence) if committed is not None else None

    @staticmethod
    def _is_newer_version(
        committed_version: tuple[int, int] | None,
        replica_version: tuple[int, int],
    ) -> bool:
        """Whether a version is committed and newer than the replica's."""
        return committed_version is not None and committed_version > replica_version

    def _record_committed_locked(
        self,
        replica: GateJobReplica,
        track_rollback: bool,
    ) -> None:
        """Record a committed replica and update the leader-address index."""
        previous = self._committed_replicas.get(replica.job_id)
        if track_rollback:
            self._commit_rollback_replicas.setdefault(replica.job_id, {}).setdefault(
                (replica.fence_token, replica.sequence), previous
            )
            self._commit_rollback_expires_at[(replica.job_id, replica.fence_token, replica.sequence)] = (
                self._clock.monotonic() + self._prepared_ttl_seconds
            )

        if previous is not None:
            self._remove_leader_index_entry(
                tuple(previous.leader_addr),
                replica.job_id,
            )

        leader_addr = tuple(replica.leader_addr)
        self._committed_sequence[replica.job_id] = replica.sequence
        self._committed_replicas[replica.job_id] = replica
        self._committed_by_leader_addr.setdefault(
            leader_addr,
            set(),
        ).add(replica.job_id)

    def _drop_committed_locked(self, job_id: str) -> None:
        """Drop a committed replica and remove its leader-address index."""
        replica = self._committed_replicas.pop(job_id, None)
        self._committed_sequence.pop(job_id, None)
        if replica is None:
            return
        self._remove_leader_index_entry(tuple(replica.leader_addr), job_id)

    def _remove_leader_index_entry(
        self,
        leader_addr: tuple[str, int],
        job_id: str,
    ) -> None:
        indexed_job_ids = self._committed_by_leader_addr.get(leader_addr)
        if indexed_job_ids is None:
            return
        indexed_job_ids.discard(job_id)
        if not indexed_job_ids:
            self._committed_by_leader_addr.pop(leader_addr, None)

    def _get_committed_replicas_for_leader_locked(
        self,
        leader_addr: tuple[str, int],
    ) -> list[GateJobReplica]:
        return [
            replica
            for job_id in self._committed_by_leader_addr.get(leader_addr, ())
            if (replica := self._committed_replicas.get(job_id)) is not None
        ]

    async def _apply_committed_with_tracking(
        self, replica: GateJobReplica
    ) -> None:
        """Record the commit under the lock, durably, then apply it locally."""
        async with self._lock:
            self._prepared.pop(replica.job_id, None)
            self._prepared_expires_at.pop(replica.job_id, None)
            self._record_committed_locked(replica, track_rollback=False)
            record = self._durable_record_locked(replica.job_id)
        await self._storage.write([record])
        await self._apply_committed(replica)

    # ------------------------------------------------------------------
    # Peer fan-out helpers
    # ------------------------------------------------------------------

    async def _send_prepare(
        self,
        peer_addr: tuple[str, int],
        payload: bytes,
        job_id: str,
    ) -> GateJobReplicaAck | None:
        try:
            response_tuple = await self._clock.wait_for(
                self._send_tcp(
                    peer_addr,
                    "gate_job_replica_prepare",
                    payload,
                    self._peer_rpc_timeout_seconds,
                ),
                timeout=self._quorum_timeout_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response := self._extract_response_bytes(response_tuple), Exception):
                raise response
        except (asyncio.TimeoutError, Exception) as error:
            await self._logger.log(
                ServerDebug(
                    message=(
                        f"Gate replication: prepare to {peer_addr} failed for "
                        f"job {job_id[:10]}: {type(error).__name__}: {error}"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )
            return None

        return self._parse_replica_ack(response)

    async def _send_commit(
        self,
        peer_addr: tuple[str, int],
        payload: bytes,
        job_id: str,
    ) -> GateJobReplicaAck | None:
        try:
            response_tuple = await self._clock.wait_for(
                self._send_tcp(
                    peer_addr,
                    "gate_job_replica_commit",
                    payload,
                    self._peer_rpc_timeout_seconds,
                ),
                timeout=self._peer_rpc_timeout_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response := self._extract_response_bytes(response_tuple), Exception):
                raise response
        except Exception as error:
            await self._logger.log(
                ServerDebug(
                    message=(
                        f"Gate replication: commit to {peer_addr} failed for "
                        f"job {job_id[:10]}: {type(error).__name__}: {error}"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )
            return None

        return self._parse_replica_ack(response)

    @staticmethod
    def _parse_replica_ack(response: bytes | None) -> GateJobReplicaAck | None:
        """Load a peer's replica ack; None for an empty answer or one that
        does not load."""
        if not response:
            return None
        try:
            return GateJobReplicaAck.load(response)
        except Exception:
            return None

    async def _abort_prepared_peers(
        self,
        peer_addrs: list[tuple[str, int]],
        replica: GateJobReplica,
    ) -> None:
        payload = GateJobReplicaAbort(
            job_id=replica.job_id,
            fence_token=replica.fence_token,
            sequence=replica.sequence,
        ).dump()
        await asyncio.gather(
            *[
                self._send_abort(peer_addr, payload, replica.job_id)
                for peer_addr in peer_addrs
            ],
            return_exceptions=True,
        )

    async def _send_abort(
        self,
        peer_addr: tuple[str, int],
        payload: bytes,
        job_id: str,
    ) -> None:
        try:
            response_tuple = await self._clock.wait_for(
                self._send_tcp(
                    peer_addr,
                    "gate_job_replica_abort",
                    payload,
                    self._peer_rpc_timeout_seconds,
                ),
                timeout=self._peer_rpc_timeout_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response := self._extract_response_bytes(response_tuple), Exception):
                raise response
        except Exception as abort_error:
            await self._logger.log(
                ServerDebug(
                    message=(
                        f"Gate replication: abort to {peer_addr} failed for "
                        f"job {job_id[:10]}: {type(abort_error).__name__}: "
                        f"{abort_error}"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )

    async def _send_fetch(
        self,
        peer_addr: tuple[str, int],
        payload: bytes,
        expected_job_id: str | None,
        expected_leader_addr: tuple[str, int] | None,
    ) -> GateJobReplicaFetchResponse | None:
        try:
            response_tuple = await self._clock.wait_for(
                self._send_tcp(
                    peer_addr,
                    "gate_job_replica_fetch",
                    payload,
                    self._peer_rpc_timeout_seconds,
                ),
                timeout=self._peer_rpc_timeout_seconds,
            )
        except Exception:
            return None

        return self._matching_fetch_response(
            self._decode_fetch_response(response_tuple),
            expected_job_id,
            expected_leader_addr,
        )

    def _decode_fetch_response(self, response_tuple: object) -> GateJobReplicaFetchResponse | None:
        """The fetch response a peer answered with; None for an error, an
        empty answer or one that does not load."""
        response = self._extract_response_bytes(response_tuple)
        if not response or isinstance(response, Exception):
            return None
        return self._parse_fetch_response(response)

    @staticmethod
    def _parse_fetch_response(response: bytes) -> GateJobReplicaFetchResponse | None:
        """Load a fetch response; None when it does not load."""
        try:
            return GateJobReplicaFetchResponse.load(response)
        except Exception:
            return None

    def _matching_fetch_response(
        self,
        loaded: GateJobReplicaFetchResponse | None,
        expected_job_id: str | None,
        expected_leader_addr: tuple[str, int] | None,
    ) -> GateJobReplicaFetchResponse | None:
        """The fetch response, when it answers for the job or leader asked."""
        if loaded is None or not self._fetch_response_matches(loaded, expected_job_id, expected_leader_addr):
            return None
        return loaded

    def _fetch_response_matches(
        self,
        loaded: GateJobReplicaFetchResponse,
        expected_job_id: str | None,
        expected_leader_addr: tuple[str, int] | None,
    ) -> bool:
        """Whether a fetch response is for the job and leader asked, each
        when one was asked."""
        return self._matches_expected(loaded.job_id, expected_job_id) and self._matches_expected(
            loaded.leader_addr, expected_leader_addr
        )

    @staticmethod
    def _matches_expected(actual: object, expected: object | None) -> bool:
        """Whether nothing was expected, or ``actual`` is what was."""
        return expected is None or actual == expected

    @staticmethod
    def _extract_response_bytes(response: object) -> bytes | Exception | None:
        """Normalize ``send_tcp`` returns to a bytes payload (or error).

        The gate's ``send_tcp`` helper returns ``(bytes_or_error, clock)``
        tuples; some send paths short-circuit to a bare ``Exception``.
        Pulling that into one place keeps the prepare/commit/abort/
        fetch paths consistent and isolates the wire-shape detail.
        """
        if isinstance(response, tuple):
            return next(iter(response), None)
        return response if isinstance(response, (bytes, Exception)) else None

    # ------------------------------------------------------------------
    # Predicates
    # ------------------------------------------------------------------

    def _is_prepare_ack_positive(self, ack_or_error: object) -> bool:
        if not isinstance(ack_or_error, GateJobReplicaAck):
            return False
        return ack_or_error.status in (
            GateJobReplicaStatus.PREPARED.value,
            GateJobReplicaStatus.ALREADY_COMMITTED.value,
            GateJobReplicaStatus.COMMITTED.value,
        )

    def _is_commit_ack_positive(self, ack_or_error: object) -> bool:
        if not isinstance(ack_or_error, GateJobReplicaAck):
            return False
        return ack_or_error.status in (
            GateJobReplicaStatus.COMMITTED.value,
            GateJobReplicaStatus.ALREADY_COMMITTED.value,
        )

    # ------------------------------------------------------------------
    # Lifecycle / maintenance
    # ------------------------------------------------------------------

    async def reap_expired_prepared(self) -> int:
        """Drop prepared entries whose TTL has elapsed.

        Returns the number of entries reaped. Intended to be called
        from a periodic maintenance loop owned by the gate server so
        prepared state held after a leader-died-mid-prepare event
        eventually frees memory even when no explicit abort arrives.
        """
        now = self._clock.monotonic()
        async with self._lock:
            reaped = self._reap_expired_prepared_locked(now)
            changed_job_ids = set(reaped) | self._reap_expired_rollbacks_locked(now)
            records = [self._durable_record_locked(job_id) for job_id in sorted(changed_job_ids)]
        if records:
            await self._storage.write(records)
        return len(reaped)

    def _reap_expired_prepared_locked(self, now: float) -> list[str]:
        """Under the lock: drop the prepared entries expired at ``now``;
        returns their job ids."""
        reaped: list[str] = []
        for job_id, expires_at in list(self._prepared_expires_at.items()):
            if expires_at <= now:
                self._prepared.pop(job_id, None)
                self._prepared_expires_at.pop(job_id, None)
                reaped.append(job_id)
        return reaped

    def _reap_expired_rollbacks_locked(self, now: float) -> set[str]:
        """Under the lock: drop the commit rollback records expired at
        ``now``; returns the jobs they were kept for."""
        reaped_job_ids: set[str] = set()
        for rollback_key, expires_at in list(
            self._commit_rollback_expires_at.items()
        ):
            if expires_at <= now:
                job_id, fence_token, sequence = rollback_key
                self._commit_rollback_expires_at.pop(rollback_key, None)
                self._drop_rollback_locked(job_id, fence_token, sequence)
                reaped_job_ids.add(job_id)
        return reaped_job_ids

    def _drop_rollback_locked(self, job_id: str, fence_token: int, sequence: int) -> None:
        """Under the lock: forget one commit's rollback record."""
        job_rollbacks = self._commit_rollback_replicas.get(job_id, {})
        job_rollbacks.pop((fence_token, sequence), None)
        if not job_rollbacks:
            self._commit_rollback_replicas.pop(job_id, None)

    def has_committed(self, job_id: str) -> bool:
        return job_id in self._committed_sequence

    def get_committed_sequence(self, job_id: str) -> int | None:
        return self._committed_sequence.get(job_id)

    async def clear_for_job(self, job_id: str) -> None:
        """Drop all replication state for ``job_id`` (terminal cleanup),
        here and in the Raft store."""
        async with self._lock:
            self._prepared.pop(job_id, None)
            self._prepared_expires_at.pop(job_id, None)
            self._attempted_sequences.pop(job_id, None)
            self._revision_locks.pop(job_id, None)
            for fence_token, sequence in self._commit_rollback_replicas.pop(job_id, {}):
                self._commit_rollback_expires_at.pop((job_id, fence_token, sequence), None)
            self._drop_committed_locked(job_id)
            record = self._durable_record_locked(job_id)
        await self._storage.write([record])

    # ------------------------------------------------------------------
    # Durability (A2-G-266)
    # ------------------------------------------------------------------

    def _durable_record_locked(self, job_id: str) -> KeyedStateRecord | KeyedStateReleasedRecord:
        """Under the lock: the job's whole state as its next store record,
        at the next version -- its release once it holds nothing."""
        self._durable_version += 1
        if self._holds_nothing_locked(job_id):
            return KeyedStateReleasedRecord(
                namespace=GATE_JOB_REPLICA_NAMESPACE, key=job_id, version=self._durable_version
            )
        return KeyedStateRecord(
            namespace=GATE_JOB_REPLICA_NAMESPACE,
            key=job_id,
            version=self._durable_version,
            state=self._state_encoder.encode(self._durable_state_locked(job_id)),
        )

    def _holds_nothing_locked(self, job_id: str) -> bool:
        """Under the lock: whether the job has no prepare, commit or rollback here."""
        return (
            job_id not in self._prepared
            and job_id not in self._committed_replicas
            and job_id not in self._commit_rollback_replicas
        )

    def _durable_state_locked(self, job_id: str) -> GateJobReplicaDurableState:
        """Under the lock: the job's two-phase-commit state as it is kept."""
        return GateJobReplicaDurableState(
            prepared=self._prepared.get(job_id),
            committed=self._committed_replicas.get(job_id),
            rollbacks=[
                GateJobReplicaRollback(fence_token=fence_token, sequence=sequence, previous=previous)
                for (fence_token, sequence), previous in self._commit_rollback_replicas.get(job_id, {}).items()
            ],
            attempted_sequence=self._attempted_sequences.get(job_id, 0),
        )

    def recover_durable_replicas(self) -> list[str]:
        """Rebuild the registries from the job states the Raft store held,
        once, at start -- before this gate answers any replica RPC: its
        prepare votes, commits and their rollbacks, and the sequences it
        sent as leader. A recovered prepare or rollback is kept a full TTL
        from now (this process's clock began at start). Returns the jobs
        holding a committed replica, for ``apply_recovered_replicas``.

        Raises:
            msgspec.DecodeError: a state this build cannot read -- written
                whole (its checksum held), in another format.
        """
        now = self._clock.monotonic()
        for job_id, record in sorted(self._storage.take_recovered_states(GATE_JOB_REPLICA_NAMESPACE).items()):
            self._durable_version = max(self._durable_version, record.version)
            if isinstance(record, KeyedStateRecord):
                self._restore_durable_state(job_id, self._state_decoder.decode(record.state), now)
        return sorted(self._committed_replicas)

    def _restore_durable_state(self, job_id: str, state: GateJobReplicaDurableState, now: float) -> None:
        """Put one recovered job state back in the registries."""
        if state.prepared is not None:
            self._hold_prepare_locked(state.prepared)
        if state.committed is not None:
            self._record_committed_locked(state.committed, track_rollback=False)
        self._restore_sequence_and_rollbacks(job_id, state, now)

    def _restore_sequence_and_rollbacks(self, job_id: str, state: GateJobReplicaDurableState, now: float) -> None:
        """Put a recovered job's attempted sequence and rollbacks back."""
        self._attempted_sequences[job_id] = state.attempted_sequence
        for rollback in state.rollbacks:
            self._commit_rollback_replicas.setdefault(job_id, {})[(rollback.fence_token, rollback.sequence)] = (
                rollback.previous
            )
            self._commit_rollback_expires_at[(job_id, rollback.fence_token, rollback.sequence)] = (
                now + self._prepared_ttl_seconds
            )

    async def apply_recovered_replicas(self, job_ids: list[str]) -> None:
        """Apply each recovered job's committed replica to the gate's state
        -- the one committed now, which a commit since start may have
        replaced."""
        for job_id in job_ids:
            async with self._lock:
                replica = self._committed_replicas.get(job_id)
            if replica is not None:
                await self._apply_committed(replica)

    def get_committed_replica(self, job_id: str) -> GateJobReplica | None:
        return self._committed_replicas.get(job_id)


__all__ = ["GateJobReplicationCoordinator"]
