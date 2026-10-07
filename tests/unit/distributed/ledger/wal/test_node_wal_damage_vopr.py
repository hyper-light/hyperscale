"""
VOPR: NodeWAL recovery under seeded on-disk damage (AD-38 Part 3.2, D-95).

Each seed appends acknowledged entries (``append`` returned: fsynced),
then damages the file in one seeded shape and reopens it:

* Torn tails -- what a power loss during an UNACKNOWLEDGED append leaves:
  a cut-short frame, a frame whose later pages were never written
  (zeros), a frame with an unwritten hole, a file extended with zeros,
  zeros alone. Recovery yields every acknowledged entry, cuts the debris
  (preserved, reported), and later appends survive the next restart.
* Truncation anywhere: recovery yields every acknowledged entry whose
  frame ends before the cut, and nothing else.
* Mid-file damage -- a bit flip, zeroed range or random overwrite inside
  an acknowledged frame with a later acknowledged frame after it (and,
  for some seeds, a torn tail after that): the WAL refuses to open with ``WALUntrustworthyError``,
  logs ``WALUntrustworthy`` at the damaged frame, and leaves the file
  byte for byte as found. A node never starts silently lacking an
  acknowledged entry.
* A read path that flips bytes never cuts or condemns: the second read
  disagrees and nothing on disk changes.
"""

import random
from pathlib import Path

import pytest

from hyperscale.distributed.ledger.storage_format import UnstableStorageReadError
from hyperscale.distributed.ledger.wal.entry_state import WALEntryState
from hyperscale.distributed.ledger.wal.node_wal import FRAME_PREFIX, WAL_FORMAT, NodeWAL
from hyperscale.distributed.ledger.wal.wal_entry import JobEventType, WALEntry
from hyperscale.distributed.ledger.wal.wal_untrustworthy_error import WALUntrustworthyError
from hyperscale.logging.hyperscale_logging_models import WALUntrustworthy
from hyperscale.logging.hyperscale_logging_models import WALTailDiscarded
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

WAL_PATH = Path("/node/ledger/node.wal")
TORN_TAIL_SEEDS = range(300)
MID_FILE_SEEDS = range(300)
MAXIMUM_ACKNOWLEDGED_ENTRIES = 12
MAXIMUM_PAYLOAD_BYTES = 96
MAXIMUM_ZERO_EXTENSION_BYTES = 4096
TORN_TAIL_SHAPES = ("cut_short", "unwritten_pages", "unwritten_hole", "zero_extended", "zeros_only")
MID_FILE_SHAPES = ("bit_flip", "zeroed_range", "random_overwrite")


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list = []

    async def log(self, entry) -> None:
        self.entries.append(entry)


def _random_payloads(random_source: random.Random, minimum_count: int) -> list[bytes]:
    return [
        random_source.randbytes(random_source.randint(0, MAXIMUM_PAYLOAD_BYTES))
        for _ in range(random_source.randint(minimum_count, MAXIMUM_ACKNOWLEDGED_ENTRIES))
    ]


async def _append_acknowledged(filesystem: SimFilesystem, payloads: list[bytes]) -> int:
    """Append every payload (each returns once fsynced); the next LSN."""
    wal = await NodeWAL.open(WAL_PATH, new_hybrid_logical_clock(), filesystem=filesystem)
    for payload in payloads:
        await wal.append(JobEventType.JOB_CREATED, payload)
    next_lsn = wal.next_lsn
    await wal.close()
    return next_lsn


async def _recovered_payloads(filesystem: SimFilesystem, logger: RecordingLogger | None = None) -> list[bytes]:
    wal = await NodeWAL.open(WAL_PATH, new_hybrid_logical_clock(), logger=logger, filesystem=filesystem)
    payloads = [entry.payload async for entry in wal.iter_from(0)]
    await wal.close()
    return payloads


def _frame_offsets(data: bytes) -> list[int]:
    """Each whole frame's starting byte, plus the end of the last one."""
    offsets = [WAL_FORMAT.header_size]
    while offsets[-1] < len(data):
        _checksum, total_length = FRAME_PREFIX.unpack_from(data, offsets[-1])
        offsets.append(offsets[-1] + total_length)
    return offsets


def _frame_offsets_end(data: bytes, frame_count: int) -> int:
    """Where the first ``frame_count`` frames of ``data`` end."""
    offset = WAL_FORMAT.header_size
    for _ in range(frame_count):
        _checksum, total_length = FRAME_PREFIX.unpack_from(data, offset)
        offset += total_length
    return offset


def _unacknowledged_frame(random_source: random.Random, lsn: int) -> bytes:
    """A frame an append in flight at the power loss was writing."""
    return WALEntry(
        lsn=lsn,
        hlc=new_hybrid_logical_clock().now(),
        state=WALEntryState.PENDING,
        event_type=JobEventType.JOB_CREATED,
        payload=random_source.randbytes(random_source.randint(1, MAXIMUM_PAYLOAD_BYTES)),
    ).to_bytes()


def _changed(original: bytes, damaged: bytearray, position: int) -> bytes:
    """``damaged``, or -- when the damage happened to rewrite identical
    bytes -- ``original`` with one bit at ``position`` flipped."""
    if bytes(damaged) == original:
        damaged[position] ^= 0x01
    return bytes(damaged)


def _torn_tail(random_source: random.Random, frame: bytes, shape: str) -> bytes:
    """The debris ``shape`` of power loss leaves of an unacknowledged ``frame``."""
    written_length = random_source.randint(1, len(frame) - 1)
    zero_extension = bytes(random_source.randint(1, MAXIMUM_ZERO_EXTENSION_BYTES))
    if shape == "cut_short":
        return frame[:written_length]
    if shape == "unwritten_pages":
        return frame[:written_length] + bytes(len(frame) - written_length)
    if shape == "unwritten_hole":
        # Past the checksum and length: a hole hiding the frame's length,
        # with written bytes after it, leaves its extent unknown -- refused,
        # never guessed at (AD-38 Part 3.2).
        hole_start = random_source.randint(FRAME_PREFIX.size, len(frame) - 1)
        hole_end = random_source.randint(hole_start + 1, len(frame))
        holed = bytearray(frame)
        holed[hole_start:hole_end] = bytes(hole_end - hole_start)
        return _changed(frame, holed, hole_start)
    if shape == "zero_extended":
        return frame[:written_length] + zero_extension
    return zero_extension


def _damage_frame(random_source: random.Random, data: bytes, frame_start: int, frame_end: int, shape: str) -> bytes:
    """``data`` with the frame at ``frame_start`` damaged in ``shape``."""
    damage_start = random_source.randrange(frame_start, frame_end)
    damage_end = random_source.randint(damage_start + 1, frame_end)
    damaged = bytearray(data)
    if shape == "bit_flip":
        damaged[damage_start] ^= 1 << random_source.randrange(8)
    elif shape == "zeroed_range":
        damaged[damage_start:damage_end] = bytes(damage_end - damage_start)
    else:
        damaged[damage_start:damage_end] = random_source.randbytes(damage_end - damage_start)
    return _changed(data, damaged, damage_start)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", TORN_TAIL_SEEDS)
async def test_a_torn_tail_recovers_every_acknowledged_entry(seed: int) -> None:
    random_source = random.Random(seed)
    filesystem = SimFilesystem()
    acknowledged = _random_payloads(random_source, minimum_count=0)
    next_lsn = await _append_acknowledged(filesystem, acknowledged)
    shape = random_source.choice(TORN_TAIL_SHAPES)
    debris = _torn_tail(random_source, _unacknowledged_frame(random_source, next_lsn), shape)
    await filesystem.append_fsync(WAL_PATH, debris)
    logger = RecordingLogger()

    assert await _recovered_payloads(filesystem, logger) == acknowledged, (seed, shape)

    (report,) = [entry for entry in logger.entries if isinstance(entry, WALTailDiscarded)]
    assert report.recovered_entries == len(acknowledged), (seed, shape)
    assert await filesystem.read_bytes(Path(report.preserved_path)) == debris, (seed, shape)
    later = _random_payloads(random_source, minimum_count=1)
    await _append_acknowledged(filesystem, later)
    assert await _recovered_payloads(filesystem) == acknowledged + later, (seed, shape)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", TORN_TAIL_SEEDS)
async def test_truncation_recovers_every_entry_before_the_cut(seed: int) -> None:
    random_source = random.Random(seed)
    filesystem = SimFilesystem()
    acknowledged = _random_payloads(random_source, minimum_count=1)
    await _append_acknowledged(filesystem, acknowledged)
    intact = await filesystem.read_bytes(WAL_PATH)
    frame_offsets = _frame_offsets(intact)
    cut = random_source.randint(WAL_FORMAT.header_size, len(intact))
    await filesystem.atomic_write(WAL_PATH, intact[:cut])

    whole_before_cut = sum(1 for frame_end in frame_offsets[1:] if frame_end <= cut)
    assert await _recovered_payloads(filesystem) == acknowledged[:whole_before_cut], (seed, cut)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", MID_FILE_SEEDS)
async def test_mid_file_damage_refuses_to_open_and_leaves_the_file_as_found(seed: int) -> None:
    random_source = random.Random(seed)
    filesystem = SimFilesystem()
    acknowledged = _random_payloads(random_source, minimum_count=1)
    next_lsn = await _append_acknowledged(filesystem, acknowledged)
    if random_source.random() < 0.5:
        debris = _torn_tail(
            random_source, _unacknowledged_frame(random_source, next_lsn), random_source.choice(TORN_TAIL_SHAPES)
        )
        await filesystem.append_fsync(WAL_PATH, debris)
    intact = await filesystem.read_bytes(WAL_PATH)
    frame_offsets = _frame_offsets(intact[: _frame_offsets_end(intact, len(acknowledged))])
    # Mid-file: a later acknowledged frame follows the damaged one. (Damage
    # to the LAST frame cannot be told from a torn append: a torn tail.)
    damageable_frames = len(acknowledged) - 1
    if damageable_frames == 0:
        return
    damaged_index = random_source.randrange(damageable_frames)
    shape = random_source.choice(MID_FILE_SHAPES)
    damaged = _damage_frame(
        random_source, intact, frame_offsets[damaged_index], frame_offsets[damaged_index + 1], shape
    )
    await filesystem.atomic_write(WAL_PATH, damaged)
    logger = RecordingLogger()

    with pytest.raises(WALUntrustworthyError) as refusal:
        await _recovered_payloads(filesystem, logger)

    assert refusal.value.damage_offset == frame_offsets[damaged_index], (seed, shape)
    (report,) = [entry for entry in logger.entries if isinstance(entry, WALUntrustworthy)]
    assert report.recovered_entries == damaged_index, (seed, shape)
    assert await filesystem.read_bytes(WAL_PATH) == damaged, (seed, shape)
    assert await filesystem.list_directory(WAL_PATH.parent, "*.discarded-*") == [], (seed, shape)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(20))
async def test_a_flipped_read_neither_cuts_nor_condemns(seed: int) -> None:
    random_source = random.Random(seed)
    filesystem = SimFilesystem()
    acknowledged = _random_payloads(random_source, minimum_count=1)
    next_lsn = await _append_acknowledged(filesystem, acknowledged)
    await filesystem.append_fsync(WAL_PATH, _unacknowledged_frame(random_source, next_lsn)[:-1])
    on_disk = await filesystem.read_bytes(WAL_PATH)
    filesystem.set_read_corruption(seed=seed, probability=1.0, path_glob="*.wal")

    with pytest.raises(UnstableStorageReadError):
        await _recovered_payloads(filesystem, RecordingLogger())

    filesystem.clear_read_corruption()
    assert await filesystem.read_bytes(WAL_PATH) == on_disk
    assert await _recovered_payloads(filesystem) == acknowledged
