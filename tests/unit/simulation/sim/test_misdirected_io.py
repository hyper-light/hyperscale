"""
B5 misdirected IO — ``SimFilesystem.set_misdirect``.

The knob models kernel/FS/firmware misdirected IO: a path-level write
lands on a seeded SIBLING file (same parent directory) instead of its
target, or a path-level read is satisfied from one. The class is
narrowed to same-directory files because the durable layout is
file-per-purpose (WAL segments, submission files, incarnation store):
sibling confusion — one WAL segment's bytes landing in another — is the
production-possible shape. The invariant the wave-2 chaos suite builds
on it: foreign bytes are caught by record framing + CRC + file-format
headers, never interpreted as valid state.

Documented scoping pinned here: handle-based sequential IO is exempt
(an open fd is bound to its inode — misdirection strikes at the
path/block layer), a read of a missing target still raises (path
resolution fails before any device IO), and a lone file proceeds
correctly (no neighbor to hit).
"""

import pytest

from tests.simulation.harness.sim import SimFilesystem

_TARGET_PATH = "/wal/segment-a.wal"
_SIBLING_PATH = "/wal/segment-b.wal"
_TARGET_CONTENT = b"segment-a-original|"
_SIBLING_CONTENT = b"segment-b-original|"


async def _build_filesystem() -> SimFilesystem:
    filesystem = SimFilesystem()
    await filesystem.append_fsync(_TARGET_PATH, _TARGET_CONTENT)
    await filesystem.append_fsync(_SIBLING_PATH, _SIBLING_CONTENT)
    return filesystem


@pytest.mark.asyncio
async def test_misdirected_append_lands_on_sibling():
    filesystem = await _build_filesystem()
    filesystem.set_misdirect(seed=17, probability=1.0)

    await filesystem.append_fsync(_TARGET_PATH, b"misdirected-append|")

    filesystem.clear_misdirect()
    # The bytes went to the neighbor; the intended target is untouched.
    assert await filesystem.read_bytes(_TARGET_PATH) == _TARGET_CONTENT
    assert await filesystem.read_bytes(_SIBLING_PATH) == (
        _SIBLING_CONTENT + b"misdirected-append|"
    )


@pytest.mark.asyncio
async def test_misdirected_atomic_write_clobbers_sibling():
    filesystem = await _build_filesystem()
    filesystem.set_misdirect(seed=17, probability=1.0)

    await filesystem.atomic_write(_TARGET_PATH, b"replacement-content")

    filesystem.clear_misdirect()
    assert await filesystem.read_bytes(_TARGET_PATH) == _TARGET_CONTENT
    assert await filesystem.read_bytes(_SIBLING_PATH) == b"replacement-content"


@pytest.mark.asyncio
async def test_misdirected_create_does_not_create_the_target():
    """A misdirected write of a NEW path lands on a sibling and the
    intended file never appears — its bytes went elsewhere."""
    filesystem = await _build_filesystem()
    filesystem.set_misdirect(seed=17, probability=1.0)

    await filesystem.append_fsync("/wal/segment-c.wal", b"new-segment|")

    filesystem.clear_misdirect()
    assert not await filesystem.exists("/wal/segment-c.wal")
    sibling_contents = {
        await filesystem.read_bytes(_TARGET_PATH),
        await filesystem.read_bytes(_SIBLING_PATH),
    }
    assert any(
        content.endswith(b"new-segment|") for content in sibling_contents
    )


@pytest.mark.asyncio
async def test_misdirected_read_returns_sibling_bytes():
    filesystem = await _build_filesystem()
    filesystem.set_misdirect(seed=17, probability=1.0)

    misdirected_read = await filesystem.read_bytes(_TARGET_PATH)

    assert misdirected_read == _SIBLING_CONTENT
    filesystem.clear_misdirect()
    assert await filesystem.read_bytes(_TARGET_PATH) == _TARGET_CONTENT


@pytest.mark.asyncio
async def test_missing_target_read_still_raises():
    """Path resolution fails before any device IO — the namei stage is
    not misdirectable."""
    filesystem = await _build_filesystem()
    filesystem.set_misdirect(seed=17, probability=1.0)

    with pytest.raises(FileNotFoundError):
        await filesystem.read_bytes("/wal/segment-missing.wal")


@pytest.mark.asyncio
async def test_lone_file_proceeds_correctly():
    """A drawn misdirect with no sibling to hit applies the operation
    to its real target — a lone file has no neighbor."""
    filesystem = SimFilesystem()
    await filesystem.append_fsync("/lonely/only.wal", b"alone|")
    filesystem.set_misdirect(seed=17, probability=1.0)

    await filesystem.append_fsync("/lonely/only.wal", b"still-here|")
    assert await filesystem.read_bytes("/lonely/only.wal") == (
        b"alone|still-here|"
    )


@pytest.mark.asyncio
async def test_handle_writes_are_exempt():
    """fd-bound IO cannot misdirect: the handle holds the inode."""
    filesystem = await _build_filesystem()
    filesystem.set_misdirect(seed=17, probability=1.0)

    append_handle = await filesystem.open(_TARGET_PATH, "ab")
    await append_handle.write(b"fd-bound|")
    await filesystem.fsync(append_handle)
    await append_handle.close()

    filesystem.clear_misdirect()
    assert await filesystem.read_bytes(_TARGET_PATH) == (
        _TARGET_CONTENT + b"fd-bound|"
    )
    assert await filesystem.read_bytes(_SIBLING_PATH) == _SIBLING_CONTENT


@pytest.mark.asyncio
async def test_misdirect_is_deterministic_per_seed():
    """Same seed: identical misdirection pattern — which ops redirect
    and which sibling each hits — proven over the full durable state.
    Different seed: a different pattern."""

    async def misdirected_run(seed: int) -> tuple:
        filesystem = SimFilesystem()
        for segment_index in range(4):
            await filesystem.append_fsync(
                f"/wal/segment-{segment_index}.wal",
                f"segment-{segment_index}|".encode(),
            )
        filesystem.set_misdirect(seed=seed, probability=0.5)
        read_results = []
        for operation_index in range(6):
            await filesystem.append_fsync(
                f"/wal/segment-{operation_index % 4}.wal",
                f"append-{operation_index}|".encode(),
            )
            read_results.append(
                await filesystem.read_bytes(
                    f"/wal/segment-{operation_index % 4}.wal"
                )
            )
        durable_state = filesystem.dump_durable()
        return read_results, durable_state

    first_run = await misdirected_run(17)
    second_run = await misdirected_run(17)
    other_seed_run = await misdirected_run(18)

    assert first_run == second_run
    assert first_run != other_seed_run


def test_misdirect_probability_validation():
    filesystem = SimFilesystem()
    with pytest.raises(ValueError):
        filesystem.set_misdirect(seed=17, probability=1.5)
    with pytest.raises(ValueError):
        filesystem.set_misdirect(seed=17, probability=-0.1)
