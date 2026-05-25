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
  Used to make re-prepare and re-commit idempotent (peer returns
  ``ALREADY_COMMITTED`` for replays at or below the committed
  sequence).
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
"""

import asyncio
import time
from typing import TYPE_CHECKING, Awaitable, Callable

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

if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.taskex import TaskRunner


class GateJobReplicationCoordinator:
    """Two-phase commit coordinator for gate job-state replication."""

    __slots__ = (
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
        prepared_ttl_seconds: float = 30.0,
        quorum_timeout_seconds: float = 5.0,
        peer_rpc_timeout_seconds: float = 5.0,
    ) -> None:
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
        self._commit_rollback_replicas: dict[
            tuple[str, int],
            GateJobReplica | None,
        ] = {}
        self._commit_rollback_expires_at: dict[tuple[str, int], float] = {}
        self._prepared_expires_at: dict[str, float] = {}
        self._prepared_ttl_seconds = prepared_ttl_seconds
        self._quorum_timeout_seconds = quorum_timeout_seconds
        self._peer_rpc_timeout_seconds = peer_rpc_timeout_seconds

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

        import sys as _sys
        _sys.stderr.write(
            f"[REPL-START self={self._get_node_addr()} job={replica.job_id[:10]}] "
            f"peers={peer_addrs} quorum={quorum_size} need={peer_acks_needed}\n"
        )
        _sys.stderr.flush()

        if peer_acks_needed == 0:
            await self._apply_committed_with_tracking(replica)
            return True

        if len(peer_addrs) < peer_acks_needed:
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

        prepare_payload = GateJobReplicaPrepare(replica=replica).dump()
        ack_results = await asyncio.gather(
            *[
                self._send_prepare(peer_addr, prepare_payload, replica.job_id)
                for peer_addr in peer_addrs
            ],
            return_exceptions=True,
        )

        acked_peers: list[tuple[str, int]] = []
        for peer_addr, ack_result in zip(peer_addrs, ack_results):
            positive = self._is_prepare_ack_positive(ack_result)
            import sys as _sys
            _sys.stderr.write(
                f"[REPL-ACK peer={peer_addr} job={replica.job_id[:10]}] "
                f"result={type(ack_result).__name__}:{ack_result!r:.120} positive={positive}\n"
            )
            _sys.stderr.flush()
            if positive:
                acked_peers.append(peer_addr)

        if len(acked_peers) < peer_acks_needed:
            await self._abort_prepared_peers(
                acked_peers, replica.job_id, replica.sequence
            )
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

        commit_payload = GateJobReplicaCommit(replica=replica).dump()
        commit_results = await asyncio.gather(
            *[
                self._send_commit(peer_addr, commit_payload, replica.job_id)
                for peer_addr in acked_peers
            ],
            return_exceptions=True,
        )

        committed_peers = [
            peer_addr
            for peer_addr, commit_result in zip(acked_peers, commit_results)
            if self._is_commit_ack_positive(commit_result)
        ]
        if len(committed_peers) < peer_acks_needed:
            await self._abort_prepared_peers(
                acked_peers,
                replica.job_id,
                replica.sequence,
            )
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

        try:
            await self._apply_committed_with_tracking(replica)
        except Exception as apply_error:
            await self._abort_prepared_peers(
                acked_peers,
                replica.job_id,
                replica.sequence,
            )
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
            async with self._lock:
                replica = self._committed_replicas.get(request.job_id)
            return GateJobReplicaFetchResponse(
                job_id=request.job_id,
                replica=replica,
                replicas=[replica] if replica is not None else [],
                found=replica is not None,
            ).dump()

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

    # ------------------------------------------------------------------
    # Orphan-recovery state-repair helper (Phase 2)
    # ------------------------------------------------------------------

    async def fetch_committed_replica_from_peers(
        self,
        job_id: str,
        peer_addrs: list[tuple[str, int]],
    ) -> GateJobReplica | None:
        """Query peers for a cached committed replica for ``job_id``.

        Returns the first replica returned by any peer (replicas are
        identical across the quorum that committed them, so picking
        the first response is correct). Returns ``None`` when no peer
        has a committed copy — typically meaning the job's quorum
        commit was lost with the original leader or never reached
        any survivor.
        """
        if not peer_addrs:
            return None

        request_payload = GateJobReplicaFetchRequest(job_id=job_id).dump()
        responses = await asyncio.gather(
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

        for response in responses:
            if isinstance(response, Exception) or response is None:
                continue
            if response.found and response.replica is not None:
                return response.replica
        return None

    async def fetch_committed_replicas_for_leader_from_peers(
        self,
        leader_addr: tuple[str, int],
        peer_addrs: list[tuple[str, int]],
    ) -> list[GateJobReplica]:
        """Fetch committed replicas whose current leader is ``leader_addr``."""
        if not peer_addrs:
            return []

        request_payload = GateJobReplicaFetchRequest(
            leader_addr=leader_addr,
        ).dump()
        responses = await asyncio.gather(
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

        replicas_by_job_id: dict[str, GateJobReplica] = {}
        for response in responses:
            if isinstance(response, Exception) or response is None:
                continue
            for replica in response.replicas:
                current = replicas_by_job_id.get(replica.job_id)
                if current is None or replica.sequence > current.sequence:
                    replicas_by_job_id[replica.job_id] = replica

        return list(replicas_by_job_id.values())

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
        for replica in replicas:
            current = replicas_by_job_id.get(replica.job_id)
            if current is None or replica.sequence > current.sequence:
                replicas_by_job_id[replica.job_id] = replica

        repaired_job_ids: list[str] = []
        for replica in replicas_by_job_id.values():
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
            ``ALREADY_COMMITTED`` when the committed sequence is at or
            above ``replica.sequence`` (idempotent replay).
            ``REJECTED`` when a strictly newer prepare is already
            in-flight for the same job.
            ``PREPARED`` when ``replica.sequence`` is newer than the
            committed sequence or no commit exists yet.
        """
        async with self._lock:
            committed_sequence = self._committed_sequence.get(replica.job_id)
            if (
                committed_sequence is not None
                and committed_sequence >= replica.sequence
            ):
                return GateJobReplicaStatus.ALREADY_COMMITTED

            existing = self._prepared.get(replica.job_id)
            if existing is not None and existing.sequence > replica.sequence:
                return GateJobReplicaStatus.REJECTED

            self._prepared[replica.job_id] = replica
            self._prepared_expires_at[replica.job_id] = (
                time.monotonic() + self._prepared_ttl_seconds
            )
            return GateJobReplicaStatus.PREPARED

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
            committed_sequence = self._committed_sequence.get(replica.job_id)
            if (
                committed_sequence is not None
                and committed_sequence >= replica.sequence
            ):
                return GateJobReplicaStatus.ALREADY_COMMITTED

            self._prepared.pop(replica.job_id, None)
            self._prepared_expires_at.pop(replica.job_id, None)
            self._record_committed_locked(replica, track_rollback=True)

        await self._apply_committed(replica)
        return GateJobReplicaStatus.COMMITTED

    async def _drop_prepared_or_committed(
        self, job_id: str, sequence: int
    ) -> GateJobReplicaStatus:
        drop_committed = False
        restore_replica: GateJobReplica | None = None
        async with self._lock:
            existing = self._prepared.get(job_id)
            if existing is not None and existing.sequence == sequence:
                self._prepared.pop(job_id, None)
                self._prepared_expires_at.pop(job_id, None)

            committed_sequence = self._committed_sequence.get(job_id)
            if committed_sequence == sequence:
                rollback_key = (job_id, sequence)
                has_rollback_record = rollback_key in self._commit_rollback_replicas
                previous_replica = self._commit_rollback_replicas.pop(
                    rollback_key,
                    None,
                )
                self._commit_rollback_expires_at.pop(rollback_key, None)
                if has_rollback_record and previous_replica is not None:
                    self._drop_committed_locked(job_id)
                    self._record_committed_locked(
                        previous_replica,
                        track_rollback=False,
                    )
                    restore_replica = previous_replica
                elif has_rollback_record:
                    self._drop_committed_locked(job_id)
                    drop_committed = True

        if restore_replica is not None:
            await self._apply_committed(restore_replica)
        elif drop_committed:
            await self._drop_committed(job_id)

        return GateJobReplicaStatus.ABORTED

    async def _apply_repair_replica(self, replica: GateJobReplica) -> bool:
        """Apply a committed replica from the repair path."""
        async with self._lock:
            committed_sequence = self._committed_sequence.get(replica.job_id)
            if (
                committed_sequence is not None
                and committed_sequence > replica.sequence
            ):
                return False
            if committed_sequence != replica.sequence:
                self._record_committed_locked(replica, track_rollback=False)

        await self._apply_committed(replica)
        return True

    def _record_committed_locked(
        self,
        replica: GateJobReplica,
        track_rollback: bool,
    ) -> None:
        """Record a committed replica and update the leader-address index."""
        previous = self._committed_replicas.get(replica.job_id)
        if track_rollback:
            rollback_key = (replica.job_id, replica.sequence)
            self._commit_rollback_replicas.setdefault(rollback_key, previous)
            self._commit_rollback_expires_at[rollback_key] = (
                time.monotonic() + self._prepared_ttl_seconds
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
        job_ids = self._committed_by_leader_addr.get(leader_addr)
        if not job_ids:
            return []
        return [
            replica
            for job_id in job_ids
            if (replica := self._committed_replicas.get(job_id)) is not None
        ]

    async def _apply_committed_with_tracking(
        self, replica: GateJobReplica
    ) -> None:
        """Apply locally and record the committed sequence under the lock."""
        async with self._lock:
            self._prepared.pop(replica.job_id, None)
            self._prepared_expires_at.pop(replica.job_id, None)
            self._record_committed_locked(replica, track_rollback=False)
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
            response_tuple = await asyncio.wait_for(
                self._send_tcp(
                    peer_addr,
                    "gate_job_replica_prepare",
                    payload,
                    self._peer_rpc_timeout_seconds,
                ),
                timeout=self._quorum_timeout_seconds,
            )
        except (asyncio.TimeoutError, Exception) as error:
            self._task_runner.run(
                self._logger.log,
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

        response = self._extract_response_bytes(response_tuple)
        if not response or isinstance(response, Exception):
            return None
        try:
            return GateJobReplicaAck.load(response)
        except Exception:
            return None

    async def _send_commit(
        self,
        peer_addr: tuple[str, int],
        payload: bytes,
        job_id: str,
    ) -> GateJobReplicaAck | None:
        try:
            response_tuple = await asyncio.wait_for(
                self._send_tcp(
                    peer_addr,
                    "gate_job_replica_commit",
                    payload,
                    self._peer_rpc_timeout_seconds,
                ),
                timeout=self._peer_rpc_timeout_seconds,
            )
        except Exception as error:
            self._task_runner.run(
                self._logger.log,
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

        response = self._extract_response_bytes(response_tuple)
        if not response or isinstance(response, Exception):
            return None
        try:
            return GateJobReplicaAck.load(response)
        except Exception:
            return None

    async def _abort_prepared_peers(
        self,
        peer_addrs: list[tuple[str, int]],
        job_id: str,
        sequence: int,
    ) -> None:
        payload = GateJobReplicaAbort(job_id=job_id, sequence=sequence).dump()
        await asyncio.gather(
            *[
                self._send_abort(peer_addr, payload, job_id)
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
            await asyncio.wait_for(
                self._send_tcp(
                    peer_addr,
                    "gate_job_replica_abort",
                    payload,
                    self._peer_rpc_timeout_seconds,
                ),
                timeout=self._peer_rpc_timeout_seconds,
            )
        except Exception as abort_error:
            self._task_runner.run(
                self._logger.log,
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
            response_tuple = await asyncio.wait_for(
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

        response = self._extract_response_bytes(response_tuple)
        if not response or isinstance(response, Exception):
            return None
        try:
            loaded = GateJobReplicaFetchResponse.load(response)
        except Exception:
            return None
        if expected_job_id is not None and loaded.job_id != expected_job_id:
            return None
        if (
            expected_leader_addr is not None
            and loaded.leader_addr != expected_leader_addr
        ):
            return None
        return loaded

    @staticmethod
    def _extract_response_bytes(response: object) -> bytes | Exception | None:
        """Normalize ``send_tcp`` returns to a bytes payload (or error).

        The gate's ``send_tcp`` helper returns ``(bytes_or_error, clock)``
        tuples; some send paths short-circuit to a bare ``Exception``.
        Pulling that into one place keeps the prepare/commit/abort/
        fetch paths consistent and isolates the wire-shape detail.
        """
        if response is None:
            return None
        if isinstance(response, tuple) and len(response) >= 1:
            return response[0]
        if isinstance(response, (bytes, Exception)):
            return response
        return None

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
        now = time.monotonic()
        reaped: list[str] = []
        async with self._lock:
            for job_id, expires_at in list(self._prepared_expires_at.items()):
                if expires_at <= now:
                    self._prepared.pop(job_id, None)
                    self._prepared_expires_at.pop(job_id, None)
                    reaped.append(job_id)
            for rollback_key, expires_at in list(
                self._commit_rollback_expires_at.items()
            ):
                if expires_at <= now:
                    self._commit_rollback_replicas.pop(rollback_key, None)
                    self._commit_rollback_expires_at.pop(rollback_key, None)
        return len(reaped)

    def has_committed(self, job_id: str) -> bool:
        return job_id in self._committed_sequence

    def get_committed_sequence(self, job_id: str) -> int | None:
        return self._committed_sequence.get(job_id)

    def clear_for_job(self, job_id: str) -> None:
        """Drop all replication state for ``job_id`` (terminal cleanup)."""
        self._prepared.pop(job_id, None)
        self._prepared_expires_at.pop(job_id, None)
        rollback_keys = [
            rollback_key
            for rollback_key in self._commit_rollback_replicas
            if rollback_key[0] == job_id
        ]
        for rollback_key in rollback_keys:
            self._commit_rollback_replicas.pop(rollback_key, None)
            self._commit_rollback_expires_at.pop(rollback_key, None)
        self._drop_committed_locked(job_id)

    def get_committed_replica(self, job_id: str) -> GateJobReplica | None:
        return self._committed_replicas.get(job_id)


__all__ = ["GateJobReplicationCoordinator"]
