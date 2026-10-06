"""
D1: a node's Raft store keeps every group's acknowledged term, vote, log
and snapshot across power loss -- and refuses a disk it cannot trust.

VOPR over the real ``RaftStore`` on ``SimFilesystem``. Each seed drives
several Raft groups through what ``RaftNode`` persists -- hard states,
appends, conflict truncations, snapshots, releases -- with writes to
different groups in flight at once (one group commit serves them), and
compactions racing them. At seeded points the disk loses power: the
restarted store must recover, for every group, its acknowledged state --
or, for a group whose write was in flight, the state before or after that
write, never anything else. A torn last record (cut in its header, cut in
its body, garbage of the right length, a zero-filled tail) is dropped and
the store resumes; writes after the resumption survive the next crash too.

A disk that cannot be explained by this identity's writes plus one torn
record is set aside, copied unread, never resumed: damage before the last
record, another identity's store, a store or identity missing its
partner, a record that breaks Raft's invariants. Only the newest
set-asides are kept. A verdict drawn from a flaky read is checked against
a second read before anything is cut or moved.
"""

from __future__ import annotations

import asyncio
import random
import struct
import zlib
from dataclasses import dataclass, field
from pathlib import Path

import msgspec
import pytest

from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp
from hyperscale.distributed.ledger.storage_format.unstable_storage_read_error import UnstableStorageReadError
from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.raft.models import RaftLogEntry
from hyperscale.distributed.raft.store import RaftStore, RaftStoreCodec
from hyperscale.distributed.raft.store.models import (
    EntriesRecord,
    GroupCreatedRecord,
    GroupReleasedRecord,
    HardStateRecord,
    RecoveredRaftGroup,
    SnapshotRecord,
    TruncateFromRecord,
)
from hyperscale.distributed.taskex import TaskRunner
from tests.simulation.harness.sim import SeededRandom, SimFilesystem

STORE_DIRECTORY = Path("/node/data/raft")
NODE_ID = "dc-east-01-10.0.0.1-09001-0000000000001"
SEEDS = range(40)
GROUPS = ("cluster:membership", "job-a", "job-b", "job-c", "job-d")
ROUNDS_PER_SEED = 60
CRASH_PROBABILITY = 0.15
TAIL_DAMAGE_PROBABILITY = 0.5
COMPACTION_PROBABILITY = 0.1
SET_ASIDE_RETAINED = 2
MEMBERS = ("member-a", "member-b", "member-c", "member-d", "member-e")


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


class SteppingClock:
    """Wall time that moves a millisecond per read: each set-aside is
    dated apart."""

    def __init__(self) -> None:
        self.now = 1_000.0

    def time(self) -> float:
        self.now += 0.001
        return self.now

    def monotonic(self) -> float:
        return self.now


@dataclass
class ModelGroup:
    """What a group's acknowledged writes say, kept apart from the codec:
    the term and vote, the snapshot point, and the log by index."""

    member_id: str = ""
    initial_voters: list[str] = field(default_factory=list)
    term: int = 0
    voted_for: str | None = None
    snapshot: SnapshotRecord | None = None
    log: dict[int, RaftLogEntry] = field(default_factory=dict)

    @property
    def base_index(self) -> int:
        return 0 if self.snapshot is None else self.snapshot.last_index

    @property
    def last_index(self) -> int:
        return max(self.log, default=self.base_index)

    def last_term(self) -> int:
        if self.log:
            return self.log[self.last_index].term
        return 0 if self.snapshot is None else self.snapshot.last_term

    def copy(self) -> ModelGroup:
        return ModelGroup(self.member_id, list(self.initial_voters), self.term, self.voted_for, self.snapshot, dict(self.log))

    def apply(self, record: object) -> None:
        match record:
            case GroupCreatedRecord(member_id=member_id, initial_voters=initial_voters):
                self.member_id = member_id
                self.initial_voters = list(initial_voters)
            case HardStateRecord(term=term, voted_for=voted_for):
                self.term, self.voted_for = term, voted_for
            case EntriesRecord(entries=entries):
                self.log.update((entry.index, entry) for entry in entries)
            case TruncateFromRecord(index=index):
                self.log = {held: entry for held, entry in self.log.items() if held < index}
            case SnapshotRecord(last_index=last_index, last_term=last_term):
                held = self.log.get(last_index)
                self.log = (
                    {index: entry for index, entry in self.log.items() if index > last_index}
                    if held is not None and held.term == last_term
                    else {}
                )
                self.snapshot = record

    def matches(self, recovered: RecoveredRaftGroup) -> bool:
        return (
            (recovered.member_id, recovered.initial_voters, recovered.term, recovered.voted_for, recovered.snapshot)
            == (self.member_id, self.initial_voters, self.term, self.voted_for, self.snapshot)
            and recovered.entries == [self.log[index] for index in sorted(self.log)]
        )


def entry(group_id: str, index: int, term: int) -> RaftLogEntry:
    return RaftLogEntry(
        term=term,
        index=index,
        command=f"{group_id}-{index}-{term}".encode(),
        command_type="vopr",
        job_id=group_id,
        hlc=HLCTimestamp(wall_ms=index, logical=term, node_id=1),
    )


def next_operation(draw: random.Random, group_id: str, group: ModelGroup) -> list[object]:
    """One write ``RaftNode`` could make next for the group, valid against
    its acknowledged state."""
    choice = draw.random()
    if not group.initial_voters:
        # A new group's first write carries its creation.
        return [
            GroupCreatedRecord(group_id=group_id, member_id=NODE_ID, initial_voters=sorted(draw.sample(MEMBERS, draw.randint(1, 3)))),
            HardStateRecord(group_id=group_id, term=1, voted_for=draw.choice((None, "member-a"))),
        ]
    if group.term == 0 or choice < 0.2:
        term = group.term + draw.choice((0, 1, 1, 2)) if group.term else 1
        voted_for = (
            group.voted_for
            if term == group.term and group.voted_for is not None
            else draw.choice((None, "member-a", "member-b"))
        )
        return [HardStateRecord(group_id=group_id, term=term, voted_for=voted_for)]
    if choice < 0.65:
        first = group.last_index + 1
        start_term = max(group.last_term(), draw.randint(group.last_term(), group.term))
        return [
            EntriesRecord(
                group_id=group_id,
                entries=[entry(group_id, first + offset, start_term) for offset in range(draw.randint(1, 4))],
            )
        ]
    if choice < 0.8 and group.last_index > group.base_index:
        # A conflict: the log is cut and a new term's entries follow.
        cut = draw.randint(group.base_index + 1, group.last_index)
        return [
            TruncateFromRecord(group_id=group_id, index=cut),
            EntriesRecord(group_id=group_id, entries=[entry(group_id, cut, group.term)]),
        ]
    if choice < 0.95 and group.last_index > group.base_index:
        last_index = draw.randint(group.base_index + 1, group.last_index)
        last_term = group.log[last_index].term
        if draw.random() < 0.2:
            # An installed snapshot beyond the log replaces it.
            last_index, last_term = group.last_index + draw.randint(1, 3), group.term
        return [
            SnapshotRecord(
                group_id=group_id,
                last_index=last_index,
                last_term=last_term,
                configuration=b'{"voters": ["member-a"]}',
                state=f"state-{last_index}".encode(),
            )
        ]
    return [GroupReleasedRecord(group_id=group_id)]


async def open_store(
    filesystem: SimFilesystem, task_runner: TaskRunner, logger: RecordingLogger, clock: SteppingClock, seed: int
) -> RaftStore:
    return RaftStore(
        directory=STORE_DIRECTORY,
        filesystem=filesystem,
        random_source=SeededRandom(seed),
        clock=clock,
        logger=logger,
        task_runner=task_runner,
        set_aside_retained=SET_ASIDE_RETAINED,
        storage_health=StorageHealth(),
    )


def damage_tail(draw: random.Random, data: bytes, codec: RaftStoreCodec) -> bytes:
    """A power loss mid-append: part of a record that was never fsynced."""
    unwritten = codec.encode_frames(
        [HardStateRecord(group_id="torn", term=99, voted_for="never-acknowledged")]
    )
    match draw.randrange(4):
        case 0:
            return data + unwritten[: draw.randint(1, 7)]
        case 1:
            return data + unwritten[: draw.randint(8, len(unwritten) - 1)]
        case 2:
            return data + unwritten[:8] + bytes(draw.randrange(256) for _ in range(len(unwritten) - 8))
        case _:
            return data + bytes(draw.randint(1, 64))


async def run_seed(seed: int) -> dict[str, int]:
    draw = random.Random(seed)
    codec = RaftStoreCodec()
    clock = SteppingClock()
    logger = RecordingLogger()
    task_runner = TaskRunner()
    filesystem = SimFilesystem()
    store = await open_store(filesystem, task_runner, logger, clock, seed)
    identity = (await store.open(NODE_ID, is_this_node=lambda node_id_full: True)).identity
    model: dict[str, ModelGroup] = {}
    counts = {"crashes": 0, "torn_tails": 0, "compactions": 0, "writes": 0, "releases": 0}
    try:
        for _round in range(ROUNDS_PER_SEED):
            in_flight: dict[str, tuple[ModelGroup, ModelGroup, asyncio.Task[None]]] = {}
            for group_id in draw.sample(GROUPS, draw.randint(1, len(GROUPS))):
                group = model.get(group_id, ModelGroup())
                records = next_operation(draw, group_id, group)
                after = group.copy()
                for record in records:
                    after.apply(record)
                released = isinstance(records[0], GroupReleasedRecord)
                in_flight[group_id] = (
                    group,
                    ModelGroup() if released else after,
                    asyncio.ensure_future(store.write(records)),
                )
                counts["releases"] += released
            if draw.random() < COMPACTION_PROBABILITY:
                in_flight["compaction"] = (ModelGroup(), ModelGroup(), asyncio.ensure_future(store.compact()))
                counts["compactions"] += 1

            if draw.random() >= CRASH_PROBABILITY:
                await asyncio.gather(*(task for _before, _after, task in in_flight.values()))
                for group_id, (_before, after, _task) in in_flight.items():
                    if group_id != "compaction":
                        model[group_id] = after
                        counts["writes"] += 1
                model = {group_id: group for group_id, group in model.items() if group != ModelGroup()}
                continue

            # Power loss at a seeded point among the writes in flight.
            for _ in range(draw.randint(0, 6)):
                await asyncio.sleep(0)
            filesystem.crash()
            durable = filesystem.dump_durable()
            acknowledged = {
                group_id for group_id, (_b, _a, task) in in_flight.items() if task.done() and not task.cancelled()
            }
            store_path = str(STORE_DIRECTORY / "store.wal")
            if draw.random() < TAIL_DAMAGE_PROBABILITY:
                durable["files"][store_path] = damage_tail(draw, durable["files"][store_path], codec)
                counts["torn_tails"] += 1
            # The old process is gone: its writer drains into the old disk.
            await asyncio.gather(*(task for _before, _after, task in in_flight.values()), return_exceptions=True)
            await store.close()
            filesystem = SimFilesystem()
            filesystem.restore_durable(durable)
            store = await open_store(filesystem, task_runner, logger, clock, seed)
            recovery = await store.open(NODE_ID, is_this_node=lambda node_id_full: True)
            counts["crashes"] += 1

            assert recovery.resumed and recovery.set_aside_reason is None, (seed, recovery.set_aside_reason)
            assert recovery.identity == identity, seed
            for group_id in sorted(set(GROUPS)):
                before = model.get(group_id, ModelGroup())
                options = [before]
                if group_id in in_flight:
                    after = in_flight[group_id][1]
                    options = [after] if group_id in acknowledged else [before, after]
                recovered = recovery.groups.get(group_id, RecoveredRaftGroup())
                matched = [option for option in options if option.matches(recovered)]
                assert matched, (seed, group_id, recovered, options)
                model[group_id] = matched[0]
            model = {group_id: group for group_id, group in model.items() if group != ModelGroup()}
    finally:
        await store.close()
        await task_runner.shutdown()
    return counts


@pytest.mark.asyncio
async def test_power_loss_never_loses_an_acknowledged_write_or_invents_one() -> None:
    totals: dict[str, int] = {}
    for seed in SEEDS:
        for name, count in (await run_seed(seed)).items():
            totals[name] = totals.get(name, 0) + count
    # The runs exercised what they are about.
    assert totals["crashes"] > len(SEEDS), totals
    assert totals["torn_tails"] > len(SEEDS) // 2, totals
    assert totals["compactions"] > len(SEEDS), totals
    assert totals["releases"] > len(SEEDS), totals


# ---------------------------------------------------------------------------
# Disks that cannot be trusted
# ---------------------------------------------------------------------------


async def written_store(filesystem: SimFilesystem, seed: int = 1) -> tuple[RaftStore, TaskRunner, RecordingLogger]:
    task_runner = TaskRunner()
    logger = RecordingLogger()
    store = await open_store(filesystem, task_runner, logger, SteppingClock(), seed)
    await store.open(NODE_ID, is_this_node=lambda node_id_full: True)
    await store.write(
        [
            GroupCreatedRecord(group_id="job-a", member_id=NODE_ID, initial_voters=["member-a"]),
            HardStateRecord(group_id="job-a", term=3, voted_for="member-a"),
        ]
    )
    await store.write([EntriesRecord(group_id="job-a", entries=[entry("job-a", 1, 3), entry("job-a", 2, 3)])])
    await store.write(
        [
            GroupCreatedRecord(group_id="job-b", member_id=NODE_ID, initial_voters=["member-a"]),
            HardStateRecord(group_id="job-b", term=1, voted_for=None),
        ]
    )
    await store.close()
    return store, task_runner, logger


async def reopen(filesystem: SimFilesystem, seed: int = 2):
    task_runner = TaskRunner()
    logger = RecordingLogger()
    store = await open_store(filesystem, task_runner, logger, SteppingClock(), seed)
    recovery = await store.open("dc-east-01-10.0.0.1-09001-00000000000ff", is_this_node=lambda node_id_full: True)
    await store.close()
    await task_runner.shutdown()
    return recovery, logger


def frame_offsets(data: bytes) -> list[int]:
    offsets, offset = [], 0
    while offset < len(data):
        offsets.append(offset)
        _checksum, length = struct.unpack_from(">II", data, offset)
        offset += 8 + length
    return offsets


async def assert_set_aside(filesystem: SimFilesystem, original: dict[str, bytes], reason_fragment: str) -> None:
    recovery, logger = await reopen(filesystem)
    assert not recovery.resumed and recovery.groups == {}
    assert recovery.set_aside_reason is not None and reason_fragment in recovery.set_aside_reason, (
        recovery.set_aside_reason
    )
    assert recovery.identity.node_id_full.endswith("ff") and recovery.identity.participation == 0
    # Copied unread, byte for byte, beside the store.
    (set_aside_directory,) = [
        directory
        for directory in await filesystem.list_subdirectories(STORE_DIRECTORY.parent)
        if directory.name.startswith("raft.set-aside.")
    ]
    for name, content in original.items():
        assert await filesystem.read_bytes(set_aside_directory / name) == content
    # The store holds only the new identity's.
    store_data = await filesystem.read_bytes(STORE_DIRECTORY / "store.wal")
    assert RaftStoreCodec().replay(store_data, recovery.identity.stamp) == ({}, len(store_data))


async def original_files(filesystem: SimFilesystem) -> dict[str, bytes]:
    return {
        name: await filesystem.read_bytes(STORE_DIRECTORY / name)
        for name in ("identity", "store.wal")
        if await filesystem.exists(STORE_DIRECTORY / name)
    }


@pytest.mark.asyncio
async def test_damage_before_the_last_record_sets_the_store_aside() -> None:
    filesystem = SimFilesystem()
    _store, task_runner, _logger = await written_store(filesystem)
    await task_runner.shutdown()
    data = bytearray(await filesystem.read_bytes(STORE_DIRECTORY / "store.wal"))
    # A bit flipped inside the second record's body: records follow it.
    second = frame_offsets(bytes(data))[1]
    data[second + 10] ^= 0x01
    await filesystem.atomic_write(STORE_DIRECTORY / "store.wal", bytes(data))

    await assert_set_aside(filesystem, await original_files(filesystem), "fails its checksum")


@pytest.mark.asyncio
async def test_another_identitys_store_is_set_aside() -> None:
    filesystem = SimFilesystem()
    _store, task_runner, _logger = await written_store(filesystem)
    await task_runner.shutdown()
    elsewhere = SimFilesystem()
    _store, task_runner, _logger = await written_store(elsewhere, seed=7)
    await task_runner.shutdown()
    await filesystem.atomic_write(
        STORE_DIRECTORY / "store.wal", await elsewhere.read_bytes(STORE_DIRECTORY / "store.wal")
    )

    await assert_set_aside(filesystem, await original_files(filesystem), "another identity")


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["identity", "store.wal"])
async def test_a_store_or_identity_without_its_partner_is_set_aside(missing: str) -> None:
    filesystem = SimFilesystem()
    _store, task_runner, _logger = await written_store(filesystem)
    await task_runner.shutdown()
    await filesystem.remove(STORE_DIRECTORY / missing)

    await assert_set_aside(filesystem, await original_files(filesystem), "without its")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("record", "fragment"),
    [
        (HardStateRecord(group_id="job-a", term=2, voted_for=None), "went from term 3"),
        (HardStateRecord(group_id="job-a", term=3, voted_for="member-b"), "went from term 3"),
        (EntriesRecord(group_id="job-a", entries=[entry("job-a", 5, 3)]), "appended index 5"),
        (EntriesRecord(group_id="job-a", entries=[entry("job-a", 3, 4)]), "appended index 3 term 4"),
        (TruncateFromRecord(group_id="job-a", index=7), "cut its log from 7"),
        (GroupCreatedRecord(group_id="job-a", member_id=NODE_ID, initial_voters=["member-b"]), "created twice"),
        (HardStateRecord(group_id="job-z", term=1, voted_for=None), "before its creation"),
    ],
)
async def test_a_whole_record_that_breaks_raft_sets_the_store_aside(record: object, fragment: str) -> None:
    """Checksums hold, so no power loss explains it."""
    filesystem = SimFilesystem()
    _store, task_runner, _logger = await written_store(filesystem)
    await task_runner.shutdown()
    data = await filesystem.read_bytes(STORE_DIRECTORY / "store.wal")
    trailing = RaftStoreCodec().encode_frames([GroupCreatedRecord(group_id="job-c", member_id=NODE_ID, initial_voters=["member-a"])])
    await filesystem.atomic_write(
        STORE_DIRECTORY / "store.wal", data + RaftStoreCodec().encode_frames([record]) + trailing
    )

    await assert_set_aside(filesystem, await original_files(filesystem), fragment)


@pytest.mark.asyncio
async def test_only_the_newest_set_asides_are_kept() -> None:
    filesystem = SimFilesystem()
    for attempt in range(SET_ASIDE_RETAINED + 2):
        _store, task_runner, _logger = await written_store(filesystem, seed=attempt + 10)
        await task_runner.shutdown()
        await filesystem.remove(STORE_DIRECTORY / "identity")
        await reopen(filesystem, seed=attempt + 20)

    set_asides = [
        directory.name
        for directory in await filesystem.list_subdirectories(STORE_DIRECTORY.parent)
        if directory.name.startswith("raft.set-aside.")
    ]
    assert len(set_asides) == SET_ASIDE_RETAINED, set_asides


@pytest.mark.asyncio
async def test_a_torn_tail_is_dropped_and_writes_after_it_survive() -> None:
    filesystem = SimFilesystem()
    _store, task_runner, _logger = await written_store(filesystem)
    await task_runner.shutdown()
    data = await filesystem.read_bytes(STORE_DIRECTORY / "store.wal")
    torn = RaftStoreCodec().encode_frames([HardStateRecord(group_id="job-a", term=9, voted_for="x")])[:-3]
    await filesystem.atomic_write(STORE_DIRECTORY / "store.wal", data + torn)

    task_runner = TaskRunner()
    logger = RecordingLogger()
    store = await open_store(filesystem, task_runner, logger, SteppingClock(), 3)
    recovery = await store.open(NODE_ID, is_this_node=lambda node_id_full: True)
    assert recovery.resumed and recovery.groups["job-a"].term == 3
    assert await filesystem.file_size(STORE_DIRECTORY / "store.wal") == len(data)
    # Appended after the cut, not after the torn bytes.
    await store.write([HardStateRecord(group_id="job-a", term=4, voted_for=None)])
    await store.close()
    await task_runner.shutdown()

    recovery, _logger = await reopen(filesystem)
    assert recovery.resumed and recovery.groups["job-a"].term == 4


@pytest.mark.asyncio
async def test_compaction_keeps_live_groups_and_drops_released_ones() -> None:
    filesystem = SimFilesystem()
    task_runner = TaskRunner()
    logger = RecordingLogger()
    store = await open_store(filesystem, task_runner, logger, SteppingClock(), 4)
    await store.open(NODE_ID, is_this_node=lambda node_id_full: True)
    for job_number in range(20):
        group_id = f"job-{job_number}"
        await store.write(
            [
                GroupCreatedRecord(group_id=group_id, member_id=NODE_ID, initial_voters=["member-a"]),
                HardStateRecord(group_id=group_id, term=1, voted_for=None),
            ]
        )
        await store.write([EntriesRecord(group_id=group_id, entries=[entry(group_id, 1, 1)])])
        if job_number:
            await store.write([GroupReleasedRecord(group_id=group_id)])
    # Released groups outweigh the live one: compaction ran on its own.
    await asyncio.sleep(0.05)
    size_after = await filesystem.file_size(STORE_DIRECTORY / "store.wal")
    await store.close()
    await task_runner.shutdown()

    recovery, _logger = await reopen(filesystem)
    assert set(recovery.groups) == {"job-0"} and recovery.groups["job-0"].entries == [entry("job-0", 1, 1)]
    live_size = len(RaftStoreCodec().materialize(recovery.groups, recovery.identity.stamp)[0])
    # Dead bytes never exceed live ones by more than the records written
    # since the last compaction.
    assert size_after <= 2 * live_size + 200, (size_after, live_size)


@pytest.mark.asyncio
async def test_a_failed_compaction_leaves_the_store_as_it_was() -> None:
    filesystem = SimFilesystem()
    task_runner = TaskRunner()
    logger = RecordingLogger()
    store = await open_store(filesystem, task_runner, logger, SteppingClock(), 5)
    await store.open(NODE_ID, is_this_node=lambda node_id_full: True)
    await store.write(
        [
            GroupCreatedRecord(group_id="job-a", member_id=NODE_ID, initial_voters=["member-a"]),
            HardStateRecord(group_id="job-a", term=2, voted_for="member-a"),
        ]
    )
    before = await filesystem.read_bytes(STORE_DIRECTORY / "store.wal")
    filesystem.set_disk_full(0)

    await store.compact()

    filesystem.clear_disk_full()
    assert await filesystem.read_bytes(STORE_DIRECTORY / "store.wal") == before
    assert any(type(entry).__name__ == "RaftStoreCompactionFailed" for entry in logger.entries)
    await store.close()
    await task_runner.shutdown()
    recovery, _logger = await reopen(filesystem)
    assert recovery.groups["job-a"].voted_for == "member-a"


def test_frames_are_checksummed_msgpack() -> None:
    frame = RaftStoreCodec().encode_frames([GroupReleasedRecord(group_id="job-a")])
    checksum, length = struct.unpack_from(">II", frame)
    assert zlib.crc32(frame[8:]) == checksum and length == len(frame) - 8
    assert msgspec.msgpack.decode(frame[8:]) == ["group_released", "job-a"]


@pytest.mark.asyncio
async def test_a_flaky_read_never_cuts_or_sets_aside_an_intact_store() -> None:
    """Bitrot on the read path, not on the platter: whichever verdict the
    flipped bytes lead to -- a torn tail or damage -- it is checked
    against a second read before anything on disk is cut or moved."""
    filesystem = SimFilesystem()
    _store, task_runner, _logger = await written_store(filesystem)
    await task_runner.shutdown()
    intact = await filesystem.read_bytes(STORE_DIRECTORY / "store.wal")
    filesystem.set_read_corruption(seed=3, probability=1.0, path_glob="*/store.wal")

    with pytest.raises(UnstableStorageReadError):
        await reopen(filesystem)

    filesystem.clear_read_corruption()
    assert await filesystem.read_bytes(STORE_DIRECTORY / "store.wal") == intact
    assert not [
        directory
        for directory in await filesystem.list_subdirectories(STORE_DIRECTORY.parent)
        if directory.name.startswith("raft.set-aside.")
    ]
    recovery, _logger = await reopen(filesystem)
    assert recovery.resumed and recovery.groups["job-a"].term == 3


@pytest.mark.asyncio
async def test_another_nodes_identity_is_set_aside() -> None:
    """A disk moved to a node at another address (or datacenter) holds an
    identity that is not this node's: resuming it would rejoin as a
    member at an address this node does not hold."""
    filesystem = SimFilesystem()
    _store, task_runner, _logger = await written_store(filesystem)
    await task_runner.shutdown()
    original = await original_files(filesystem)
    task_runner = TaskRunner()
    store = await open_store(filesystem, task_runner, RecordingLogger(), SteppingClock(), 9)

    recovery = await store.open("dc-west-01-10.0.0.9-09001-00000000000aa", is_this_node=lambda node_id_full: False)
    await store.close()
    await task_runner.shutdown()

    assert not recovery.resumed and "is not this node" in recovery.set_aside_reason
    (set_aside_directory,) = [
        directory
        for directory in await filesystem.list_subdirectories(STORE_DIRECTORY.parent)
        if directory.name.startswith("raft.set-aside.")
    ]
    assert await filesystem.read_bytes(set_aside_directory / "identity") == original["identity"]
