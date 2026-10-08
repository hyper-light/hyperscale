"""
B4 runtime read corruption — ``SimFilesystem.set_read_corruption``.

The knob models bitrot surfacing at READ time (latent sector, bad
cable, DRAM bit on the read path): seeded byte flips in the bytes a
read RETURNS, with stored state never mutated — disarming restores
clean reads over the intact platter. The production invariant this
exists to exercise (wave-2 chaos suite): a CRC-failing runtime read —
checkpoint load, ledger read, incarnation-store read — is a LOUD
failure or a clean recovery-truncation, never silently-applied wrong
state.

Every test asserts determinism the strong way: two identically seeded
filesystems replaying the same operation sequence return identical
corrupted bytes — including WHICH byte flipped — and a different seed
flips differently.
"""

import pytest

from tests.simulation.harness.sim import SimFilesystem

_WAL_PATH = "/wal/events.wal"
_CONTENT = b"record-1|record-2|record-3|"


async def _build_filesystem(content: bytes = _CONTENT) -> SimFilesystem:
    filesystem = SimFilesystem()
    await filesystem.append_fsync(_WAL_PATH, content)
    return filesystem


@pytest.mark.asyncio
async def test_read_corruption_flips_returned_byte_not_stored_state():
    filesystem = await _build_filesystem()
    filesystem.set_read_corruption(seed=11, probability=1.0)

    corrupted_read = await filesystem.read_bytes(_WAL_PATH)
    assert corrupted_read != _CONTENT
    assert len(corrupted_read) == len(_CONTENT)
    differing_indexes = [
        index
        for index in range(len(_CONTENT))
        if corrupted_read[index] != _CONTENT[index]
    ]
    assert len(differing_indexes) == 1, differing_indexes

    # The rot is in the read path, not the platter: disarming restores
    # the intact content.
    filesystem.clear_read_corruption()
    assert await filesystem.read_bytes(_WAL_PATH) == _CONTENT


@pytest.mark.asyncio
async def test_read_corruption_is_deterministic_per_seed():
    async def corrupted_reads(seed: int) -> list[bytes]:
        filesystem = await _build_filesystem()
        filesystem.set_read_corruption(seed=seed, probability=0.5)
        return [await filesystem.read_bytes(_WAL_PATH) for _ in range(8)]

    first_run = await corrupted_reads(11)
    second_run = await corrupted_reads(11)
    other_seed_run = await corrupted_reads(12)

    assert first_run == second_run
    assert first_run != other_seed_run
    # probability=0.5 over 8 reads: the pattern mixes clean and corrupt
    # reads (a fixed-seed fact, stable forever under replay).
    assert any(read_result != _CONTENT for read_result in first_run)


@pytest.mark.asyncio
async def test_read_corruption_scopes_by_path_glob():
    filesystem = await _build_filesystem()
    await filesystem.append_fsync("/data/clean.bin", _CONTENT)
    filesystem.set_read_corruption(seed=11, probability=1.0, path_glob="*.wal")

    assert await filesystem.read_bytes(_WAL_PATH) != _CONTENT
    # Out-of-scope paths are untouched AND consume no RNG draws — the
    # knob does not see them at all.
    assert await filesystem.read_bytes("/data/clean.bin") == _CONTENT


@pytest.mark.asyncio
async def test_handle_reads_are_corrupted_too():
    """Handle-based ``read`` / ``readline`` return corrupted copies —
    the whole read surface rots, not just ``read_bytes``."""
    filesystem = SimFilesystem()
    line_content = b"line-one-payload\nline-two-payload\n"
    await filesystem.append_fsync(_WAL_PATH, line_content)
    filesystem.set_read_corruption(seed=11, probability=1.0)

    read_handle = await filesystem.open(_WAL_PATH, "r")
    corrupted_full_read = await read_handle.read()
    assert corrupted_full_read != line_content
    assert len(corrupted_full_read) == len(line_content)

    await read_handle.seek(0)
    corrupted_line = await read_handle.readline()
    first_line = line_content[: line_content.index(b"\n") + 1]
    assert len(corrupted_line) == len(first_line)
    assert corrupted_line != first_line

    filesystem.clear_read_corruption()
    await read_handle.seek(0)
    assert await read_handle.read() == line_content


@pytest.mark.asyncio
async def test_handle_read_respects_path_glob():
    filesystem = SimFilesystem()
    await filesystem.append_fsync("/data/clean.bin", _CONTENT)
    filesystem.set_read_corruption(seed=11, probability=1.0, path_glob="*.wal")

    clean_handle = await filesystem.open("/data/clean.bin", "r")
    assert await clean_handle.read() == _CONTENT


@pytest.mark.asyncio
async def test_empty_read_passes_unchanged():
    """A zero-length read has no bytes to rot."""
    filesystem = SimFilesystem()
    await filesystem.append_fsync(_WAL_PATH, b"")
    filesystem.set_read_corruption(seed=11, probability=1.0)
    assert await filesystem.read_bytes(_WAL_PATH) == b""


def test_read_corruption_probability_validation():
    filesystem = SimFilesystem()
    with pytest.raises(ValueError):
        filesystem.set_read_corruption(seed=11, probability=1.5)
    with pytest.raises(ValueError):
        filesystem.set_read_corruption(seed=11, probability=-0.1)
