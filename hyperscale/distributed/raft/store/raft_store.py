"""
A node's Raft store (D1): one durable, group-committed log of every Raft
group's persistent state, under an identity the node can prove is its own.
"""

import asyncio
import zlib
from collections.abc import Callable
from pathlib import Path

import msgspec

from hyperscale.distributed.ledger.storage_format.set_aside import require_stable_read
from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.ledger.wal.wal_writer import WALWriter, WALWriterConfig, WriteRequest
from hyperscale.distributed.runtime import Clock, Filesystem, Random
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    RaftStoreCompacted,
    RaftStoreCompactionFailed,
    RaftStoreOpened,
    RaftStoreSetAside,
)

from .models import (
    GroupReleasedRecord,
    HardStateRecord,
    KeyedStateRecord,
    KeyedStateReleasedRecord,
    RaftIdentity,
    RaftStoreHeader,
    RaftStoreRecovery,
    RecoveredRaftGroup,
    SnapshotRecord,
)
from .raft_store_codec import (
    FRAME_HEADER,
    STORE_FORMAT_VERSION,
    RaftStoreCodec,
    RaftStoreRecord,
    RecoveredKeyedStates,
)
from .raft_store_untrustworthy_error import RaftStoreUntrustworthyError

IDENTITY_FORMAT_VERSION = 1
IDENTITY_FILE_NAME = "identity"
STORE_FILE_NAME = "store.wal"
SET_ASIDE_SUFFIX = ".set-aside."
# The identity stamp: 128 random bits, so two identities never share one.
STAMP_BITS = 128


class RaftStore:
    """Every Raft group's term, vote, log and snapshot on this node, in
    one file that group-commits across groups (D1 P2) -- under an identity
    stamp every store file must carry (P4).

    ``open`` resumes the identity and groups an intact disk holds, makes a
    new identity on an empty one, and sets an untrustworthy one aside
    (P5-P6). ``write`` returns once its records are durable, and raises
    when they are not: a Raft reply that depends on them must not be sent.
    The file is rewritten live-only once its dead bytes outweigh its live
    ones (P11).
    """

    __slots__ = (
        "_directory",
        "_filesystem",
        "_random_source",
        "_clock",
        "_logger",
        "_task_runner",
        "_set_aside_retained",
        "_storage_health",
        "_codec",
        "_encoder",
        "_writer",
        "_identity",
        "_group_bytes",
        "_hard_state_bytes",
        "_state_bytes",
        "_live_bytes",
        "_dead_bytes",
        "_compaction_token",
        "_closed",
        "_recovered_groups",
        "_recovered_states",
    )

    def __init__(
        self,
        directory: Path,
        filesystem: Filesystem,
        random_source: Random,
        clock: Clock,
        logger: Logger,
        task_runner: TaskRunner,
        set_aside_retained: int,
        storage_health: StorageHealth,
    ) -> None:
        """
        Args:
            directory: The store's own directory under the node's data dir
            filesystem: The node's storage seam
            random_source: Draws a new identity's stamp
            clock: Dates a set-aside
            logger: The node's logger
            task_runner: Runs compactions
            set_aside_retained: How many of the newest set-asides to keep
            storage_health: The node's shared storage health
        """
        self._directory = directory
        self._filesystem = filesystem
        self._random_source = random_source
        self._clock = clock
        self._logger = logger
        self._task_runner = task_runner
        self._set_aside_retained = set_aside_retained
        self._storage_health = storage_health
        self._codec = RaftStoreCodec()
        self._encoder = msgspec.msgpack.Encoder()
        self._writer: WALWriter | None = None
        self._identity: RaftIdentity | None = None
        # Bytes of each live group's records, and of those its latest hard
        # state's; what superseded or released records hold is dead. The
        # counts are estimates between compactions (a truncation's dead
        # entries are not subtracted) and exact after each.
        self._group_bytes: dict[str, int] = {}
        self._hard_state_bytes: dict[str, int] = {}
        # Bytes of each held keyed state's latest record.
        self._state_bytes: dict[tuple[str, str], int] = {}
        self._live_bytes = 0
        self._dead_bytes = 0
        self._compaction_token: str | None = None
        self._closed = False
        # Groups the disk held, until their coordinators take them.
        self._recovered_groups: dict[str, RecoveredRaftGroup] = {}
        # Keyed states the disk held, until their owners take them.
        self._recovered_states: RecoveredKeyedStates = {}

    @property
    def identity(self) -> RaftIdentity:
        if self._identity is None:
            raise RuntimeError("the Raft store is not open")
        return self._identity

    @property
    def durable(self) -> bool:
        return True

    @property
    def participation(self) -> int:
        return self.identity.participation

    def take_recovered_groups(self, belongs: Callable[[str], bool]) -> dict[str, RecoveredRaftGroup]:
        """The recovered groups ``belongs`` claims, handed over once."""
        taken = self._claimed_recovered_groups(belongs)
        for group_id in taken:
            del self._recovered_groups[group_id]
        return taken

    def _claimed_recovered_groups(self, belongs: Callable[[str], bool]) -> dict[str, RecoveredRaftGroup]:
        """The recovered groups ``belongs`` claims, still held here."""
        return {group_id: group for group_id, group in self._recovered_groups.items() if belongs(group_id)}

    def take_recovered_states(self, namespace: str) -> dict[str, KeyedStateRecord | KeyedStateReleasedRecord]:
        """Each key of ``namespace`` the disk held -- its state, or the
        release that ended it, whose version the owner's next write must
        exceed -- handed over once."""
        taken = self._claimed_recovered_states(namespace)
        for key in taken:
            del self._recovered_states[(namespace, key)]
        return taken

    def _claimed_recovered_states(self, namespace: str) -> dict[str, KeyedStateRecord | KeyedStateReleasedRecord]:
        """The recovered keyed states of ``namespace``, still held here."""
        return {
            key: record
            for (record_namespace, key), record in self._recovered_states.items()
            if record_namespace == namespace
        }

    @property
    def path(self) -> Path:
        return self._directory / STORE_FILE_NAME

    async def open(self, fresh_node_id_full: str, is_this_node: Callable[[str], bool]) -> RaftStoreRecovery:
        """Resume this disk's identity and groups, or make a new identity
        (``fresh_node_id_full``) when the disk holds none or one it cannot
        trust -- which it first sets aside. An identity ``is_this_node``
        refuses (another node's: another address or datacenter) is not
        trusted either."""
        identity_path = self._directory / IDENTITY_FILE_NAME
        store_path = self.path
        await self._filesystem.mkdir(self._directory, parents=True, exist_ok=True)
        groups, states, torn_bytes, set_aside_reason, resumed = await self._recover_or_make_identity(
            identity_path, store_path, fresh_node_id_full, is_this_node
        )
        await self._start_writer(store_path, groups, states, resumed, torn_bytes)
        self._recovered_groups = dict(groups)
        self._recovered_states = dict(states)
        return RaftStoreRecovery(
            identity=self.identity,
            groups=groups,
            resumed=resumed,
            set_aside_reason=set_aside_reason,
        )

    async def _recover_or_make_identity(
        self,
        identity_path: Path,
        store_path: Path,
        fresh_node_id_full: str,
        is_this_node: Callable[[str], bool],
    ) -> tuple[dict[str, RecoveredRaftGroup], RecoveredKeyedStates, int, str | None, bool]:
        """Resume what the disk holds, else make a new identity (P5-P6):
        the groups, torn bytes dropped, set-aside reason and whether it
        resumed."""
        recovered = await self._recover_existing(identity_path, store_path, fresh_node_id_full, is_this_node)
        if not recovered[4]:
            await self._make_identity(identity_path, store_path, fresh_node_id_full)
        return recovered

    async def _recover_existing(
        self,
        identity_path: Path,
        store_path: Path,
        fresh_node_id_full: str,
        is_this_node: Callable[[str], bool],
    ) -> tuple[dict[str, RecoveredRaftGroup], RecoveredKeyedStates, int, str | None, bool]:
        """Resume (or set aside) the store files the disk holds; nothing
        is recovered from a disk holding neither."""
        identity_exists = await self._filesystem.exists(identity_path)
        store_exists = await self._filesystem.exists(store_path)
        if identity_exists or store_exists:
            return await self._resume_or_set_aside(
                identity_path, store_path, identity_exists, store_exists, fresh_node_id_full, is_this_node
            )
        return {}, {}, 0, None, False

    async def _resume_or_set_aside(
        self,
        identity_path: Path,
        store_path: Path,
        identity_exists: bool,
        store_exists: bool,
        fresh_node_id_full: str,
        is_this_node: Callable[[str], bool],
    ) -> tuple[dict[str, RecoveredRaftGroup], RecoveredKeyedStates, int, str | None, bool]:
        """Resume the store, or set an untrustworthy one aside (P5-P6)."""
        # The bytes each verdict below is drawn from.
        verdict_reads: dict[Path, bytes] = {}
        try:
            groups, states, torn_bytes = await self._resume(
                identity_path, store_path, identity_exists, store_exists, fresh_node_id_full, is_this_node, verdict_reads
            )
        except RaftStoreUntrustworthyError as untrustworthy:
            set_aside_reason = str(untrustworthy)
            await self._set_aside(set_aside_reason, fresh_node_id_full, verdict_reads)
            return {}, {}, 0, set_aside_reason, False
        return groups, states, torn_bytes, None, True

    async def _resume(
        self,
        identity_path: Path,
        store_path: Path,
        identity_exists: bool,
        store_exists: bool,
        fresh_node_id_full: str,
        is_this_node: Callable[[str], bool],
        verdict_reads: dict[Path, bytes],
    ) -> tuple[dict[str, RecoveredRaftGroup], RecoveredKeyedStates, int]:
        """Adopt the disk's identity and replay its store: the groups, the
        keyed states and the torn bytes cut. Raises
        RaftStoreUntrustworthyError."""
        self._require_paired_files(identity_exists, store_exists)
        identity = await self._read_identity(identity_path, fresh_node_id_full, is_this_node, verdict_reads)
        groups, states, torn_bytes = await self._replay_store(store_path, identity.stamp, verdict_reads)
        self._identity = identity
        return groups, states, torn_bytes

    @staticmethod
    def _require_paired_files(identity_exists: bool, store_exists: bool) -> None:
        """An identity and its store exist together or not at all (P4)."""
        if not identity_exists:
            raise RaftStoreUntrustworthyError("a store without its identity")
        if not store_exists:
            raise RaftStoreUntrustworthyError("an identity without its store")

    async def _read_identity(
        self,
        identity_path: Path,
        fresh_node_id_full: str,
        is_this_node: Callable[[str], bool],
        verdict_reads: dict[Path, bytes],
    ) -> RaftIdentity:
        """The disk's identity, refused unless it decodes, is this build's
        format and is this node's."""
        verdict_reads[identity_path] = await self._filesystem.read_bytes(identity_path)
        identity = self._decode_identity(verdict_reads[identity_path])
        if identity.format_version != IDENTITY_FORMAT_VERSION:
            raise RaftStoreUntrustworthyError(
                f"the identity is format {identity.format_version}; this build reads {IDENTITY_FORMAT_VERSION}"
            )
        if not is_this_node(identity.node_id_full):
            raise RaftStoreUntrustworthyError(
                f"the identity {identity.node_id_full} is not this node ({fresh_node_id_full})"
            )
        return identity

    @staticmethod
    def _decode_identity(identity_data: bytes) -> RaftIdentity:
        """Decode an identity; one that does not decode is untrustworthy."""
        try:
            return msgspec.msgpack.decode(identity_data, type=RaftIdentity)
        except msgspec.DecodeError as decode_error:
            raise RaftStoreUntrustworthyError(f"the identity does not decode: {decode_error}") from decode_error

    async def _replay_store(
        self,
        store_path: Path,
        stamp: bytes,
        verdict_reads: dict[Path, bytes],
    ) -> tuple[dict[str, RecoveredRaftGroup], RecoveredKeyedStates, int]:
        """Replay the store's groups and keyed states, cutting a torn
        tail: the groups, the states and the bytes cut."""
        store_data = verdict_reads[store_path] = await self._filesystem.read_bytes(store_path)
        groups, states, whole_length = self._codec.replay(store_data, stamp)
        if (torn_bytes := len(store_data) - whole_length) > 0:
            # Only when a second read agrees: a flipped read must
            # never cut an acknowledged record.
            await require_stable_read(self._filesystem, store_path, store_data)
            await self._filesystem.truncate(store_path, whole_length)
        return groups, states, torn_bytes

    async def _make_identity(self, identity_path: Path, store_path: Path, fresh_node_id_full: str) -> None:
        """Make a new store and identity under a fresh stamp (P4)."""
        stamp = self._random_source.randrange(0, 1 << STAMP_BITS).to_bytes(STAMP_BITS // 8, "big")
        # The store is made first: a crash before the identity leaves a
        # store without one, which the next start sets aside.
        await self._filesystem.atomic_write(
            store_path,
            self._codec.encode_frames([RaftStoreHeader(stamp=stamp, format_version=STORE_FORMAT_VERSION)]),
        )
        self._identity = RaftIdentity(
            format_version=IDENTITY_FORMAT_VERSION,
            node_id_full=fresh_node_id_full,
            participation=0,
            stamp=stamp,
        )
        await self._filesystem.atomic_write(identity_path, msgspec.msgpack.encode(self._identity))

    async def _start_writer(
        self,
        store_path: Path,
        groups: dict[str, RecoveredRaftGroup],
        states: RecoveredKeyedStates,
        resumed: bool,
        torn_bytes: int,
    ) -> None:
        """Count the store's live and dead bytes, start its group-commit
        writer and log the open."""
        live_store, self._group_bytes, self._hard_state_bytes, self._state_bytes = self._codec.materialize(
            groups, states, self.identity.stamp
        )
        self._live_bytes = len(live_store)
        self._dead_bytes = await self._filesystem.file_size(store_path) - self._live_bytes
        self._writer = WALWriter(
            path=store_path,
            config=WALWriterConfig(),
            logger=self._logger,
            filesystem=self._filesystem,
            storage_health=self._storage_health,
        )
        await self._writer.start()
        await self._logger.log(
            RaftStoreOpened(
                message=(
                    f"Resumed as {self.identity.node_id_full} with {len(groups)} Raft groups"
                    if resumed
                    else f"Raft store made for {self.identity.node_id_full}"
                ),
                node_id=self.identity.node_id_full,
                path=str(store_path),
                resumed=resumed,
                groups_recovered=len(groups),
                torn_bytes_dropped=torn_bytes,
            )
        )

    async def write(self, records: list[RaftStoreRecord]) -> None:
        """Make ``records`` durable, in order, as one group-committed write.

        Raises:
            Exception: the write did not become durable (the device failed,
                the writer refused it, or the store is closed).
        """
        # None until open, and again from the moment close begins.
        writer = self._writer
        if writer is None:
            raise RuntimeError("the Raft store is not open")
        frames = bytearray()
        record_sizes: list[int] = []
        encode = self._encoder.encode
        for record in records:
            body = encode(record)
            frames += FRAME_HEADER.pack(zlib.crc32(body), len(body))
            frames += body
            record_sizes.append(FRAME_HEADER.size + len(body))
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        writer.submit(WriteRequest(data=bytes(frames), future=future))
        await future

        group_bytes = self._group_bytes
        for record, record_size in zip(records, record_sizes):
            match record:
                case HardStateRecord(group_id=group_id):
                    superseded = self._hard_state_bytes.get(group_id, 0)
                    self._hard_state_bytes[group_id] = record_size
                    group_bytes[group_id] = group_bytes.get(group_id, 0) - superseded + record_size
                    self._dead_bytes += superseded
                    self._live_bytes += record_size - superseded
                case SnapshotRecord(group_id=group_id):
                    # What the snapshot covers is dead; its hard state lives.
                    covered = group_bytes.get(group_id, 0) - self._hard_state_bytes.get(group_id, 0)
                    group_bytes[group_id] = self._hard_state_bytes.get(group_id, 0) + record_size
                    self._dead_bytes += covered
                    self._live_bytes += record_size - covered
                case GroupReleasedRecord(group_id=group_id):
                    released = group_bytes.pop(group_id, 0)
                    self._hard_state_bytes.pop(group_id, None)
                    self._dead_bytes += released + record_size
                    self._live_bytes -= released
                case KeyedStateRecord() | KeyedStateReleasedRecord():
                    self._account_keyed_state(record, record_size)
                case _:
                    group_bytes[record.group_id] = group_bytes.get(record.group_id, 0) + record_size
                    self._live_bytes += record_size
        if self._dead_bytes > self._live_bytes and self._compaction_token is None:
            self._compaction_token = self._task_runner.run(self.compact, alias="raft-store-compaction").token

    def _account_keyed_state(self, record: KeyedStateRecord | KeyedStateReleasedRecord, record_size: int) -> None:
        """Count a keyed-state record's bytes: its latest record holds all
        of a state, so the one it supersedes -- or, released, the state's
        last one and the release itself -- is dead."""
        state_key = (record.namespace, record.key)
        superseded = self._state_bytes.pop(state_key, 0)
        self._dead_bytes += superseded
        self._live_bytes -= superseded
        match record:
            case KeyedStateRecord():
                self._state_bytes[state_key] = record_size
                self._live_bytes += record_size
            case KeyedStateReleasedRecord():
                self._dead_bytes += record_size

    async def compact(self) -> None:
        """Rewrite the store with only what its live groups need (P10):
        atomically, with no group commit in flight. A failure leaves the
        store as it was, logged; the next compaction retries."""
        writer = self._writer
        stamp = self.identity.stamp
        rewritten: list[tuple[int, dict[str, int], dict[str, int], dict[tuple[str, str], int]]] = []

        def live_only(committed: bytes) -> bytes:
            groups, states, _whole_length = self._codec.replay(committed, stamp)
            data, group_bytes, hard_state_bytes, state_bytes = self._codec.materialize(groups, states, stamp)
            rewritten.append((len(data), group_bytes, hard_state_bytes, state_bytes))
            return data

        try:
            if writer is None:
                return
            try:
                reclaimed = await writer.rewrite(live_only)
            except (OSError, RaftStoreUntrustworthyError) as compaction_error:
                await self._logger.log(
                    RaftStoreCompactionFailed(
                        message=f"Raft store compaction failed: {compaction_error}",
                        node_id=self.identity.node_id_full,
                        path=str(self.path),
                        error=str(compaction_error),
                    )
                )
                return
            ((self._live_bytes, self._group_bytes, self._hard_state_bytes, self._state_bytes),) = rewritten
            self._dead_bytes = 0
            await self._logger.log(
                RaftStoreCompacted(
                    message=f"Raft store compacted: {reclaimed} bytes reclaimed, {self._live_bytes} live",
                    node_id=self.identity.node_id_full,
                    path=str(self.path),
                    bytes_reclaimed=reclaimed,
                    live_bytes=self._live_bytes,
                )
            )
        finally:
            self._compaction_token = None

    async def advance_participation(self) -> RaftIdentity:
        """This node left a membership group for good (``_abandon``): it
        comes back, if ever, under the next participation -- durably,
        before it takes part again."""
        identity = self.identity
        self._identity = RaftIdentity(
            format_version=identity.format_version,
            node_id_full=identity.node_id_full,
            participation=identity.participation + 1,
            stamp=identity.stamp,
        )
        await self._filesystem.atomic_write(self._directory / IDENTITY_FILE_NAME, msgspec.msgpack.encode(self._identity))
        return self._identity

    async def close(self) -> None:
        """Stop the writer once its pending writes are committed (a
        compaction under way is cancelled; it is atomic)."""
        if self._closed:
            return
        self._closed = True
        # No write is accepted from here on: one is refused once the
        # writer is gone.
        writer, self._writer = self._writer, None
        await self._cancel_compaction()
        if writer is not None:
            await writer.stop()

    async def _set_aside(self, reason: str, node_id_full: str, verdict_reads: dict[Path, bytes]) -> None:
        """Copy this store's files, unread beyond what proved it
        untrustworthy, to a dated directory beside it; remove them here
        (the identity first, so a crash midway leaves a store without an
        identity -- set aside again); keep only the newest set-asides."""
        await self._require_stable_verdict(verdict_reads)
        held_files, held_contents = await self._read_held_files()
        parent = self._directory.parent
        prefix = f"{self._directory.name}{SET_ASIDE_SUFFIX}"
        set_asides = await self._existing_set_asides(parent, prefix)
        set_aside_at = self._next_set_aside_at(set_asides)
        set_aside_directory = parent / f"{prefix}{set_aside_at}"
        set_asides.append((set_aside_at, set_aside_directory))
        await self._filesystem.mkdir(set_aside_directory, parents=True, exist_ok=True)
        await self._copy_held_files(set_aside_directory, held_contents)
        await self._remove_held_files(held_files)
        await self._logger.log(
            RaftStoreSetAside(
                message=f"Raft store set aside to {set_aside_directory}: {reason}",
                node_id=node_id_full,
                path=str(self._directory),
                set_aside_path=str(set_aside_directory),
                reason=reason,
            )
        )
        await self._prune_set_asides(set_asides)

    async def _require_stable_verdict(self, verdict_reads: dict[Path, bytes]) -> None:
        """Re-read every file a set-aside verdict was drawn from."""
        # The verdict stands only if what it was drawn from reads the same
        # again: one drawn from a flipped read must not set an intact store
        # aside.
        for verdict_path, verdict_content in verdict_reads.items():
            await require_stable_read(self._filesystem, verdict_path, verdict_content)

    async def _read_held_files(self) -> tuple[list[Path], dict[Path, bytes]]:
        """The store directory's files and each one's bytes."""
        held_files = await self._filesystem.list_directory(self._directory)
        held_contents = {held_file: await self._filesystem.read_bytes(held_file) for held_file in held_files}
        return held_files, held_contents

    @staticmethod
    def _is_set_aside_directory(directory: Path, prefix: str) -> bool:
        """Whether ``directory`` is a dated set-aside of this store."""
        return directory.name.startswith(prefix) and directory.name.removeprefix(prefix).isdigit()

    async def _existing_set_asides(self, parent: Path, prefix: str) -> list[tuple[int, Path]]:
        """This store's earlier set-asides, oldest first, with their dates."""
        return sorted(
            (int(directory.name.removeprefix(prefix)), directory)
            for directory in await self._filesystem.list_subdirectories(parent)
            if self._is_set_aside_directory(directory, prefix)
        )

    def _next_set_aside_at(self, set_asides: list[tuple[int, Path]]) -> int:
        """The new set-aside's date in wall milliseconds."""
        # Dated by wall time, but always after every earlier set-aside: a
        # clock stepped back across restarts must not merge two of them.
        return max(int(self._clock.time() * 1000), set_asides[-1][0] + 1 if set_asides else 0)

    async def _copy_held_files(self, set_aside_directory: Path, held_contents: dict[Path, bytes]) -> None:
        """Write each held file's bytes into the set-aside directory."""
        for held_file, content in held_contents.items():
            await self._filesystem.atomic_write(set_aside_directory / held_file.name, content)

    async def _remove_held_files(self, held_files: list[Path]) -> None:
        """Remove the held files, the identity first: a crash midway leaves
        a store without an identity, set aside again."""
        for held_file in sorted(held_files, key=lambda held: held.name != IDENTITY_FILE_NAME):
            await self._filesystem.remove(held_file)

    async def _prune_set_asides(self, set_asides: list[tuple[int, Path]]) -> None:
        """Remove all but the newest ``set_aside_retained`` set-asides."""
        for _set_aside_at, superseded in set_asides[: max(0, len(set_asides) - self._set_aside_retained)]:
            for superseded_file in await self._filesystem.list_directory(superseded):
                await self._filesystem.remove(superseded_file)
            await self._filesystem.remove_directory(superseded)

    async def _cancel_compaction(self) -> None:
        """Cancel a compaction under way (it is atomic)."""
        if (compaction_token := self._compaction_token) is not None:
            await self._task_runner.cancel(compaction_token)
