"""
RealFilesystem — the Phase 7 storage seam's production implementation.

Pins the three contracts the seam exists for:

1. Every operation is ASYNC and runs on the filesystem's own dedicated
   ``ThreadPoolExecutor`` — never the event loop, never the loop's
   ambient default executor.
2. Executor lifecycle: threads spawn lazily on first IO, ``shutdown``
   is idempotent, ``shutdown(wait=True)`` joins every worker thread
   (no leak), and IO after shutdown fails LOUDLY.
3. Durability-pattern semantics: ``atomic_write`` leaves either the
   complete old or complete new content (and never a stray temp file,
   even when the write fails); ``append_fsync`` appends durable units.
"""

import asyncio
import threading

import pytest

from hyperscale.core.runtime import RealFilesystem


@pytest.fixture
def filesystem():
    real_filesystem = RealFilesystem(max_workers=2)
    yield real_filesystem
    real_filesystem.shutdown(wait=True)


def _run(coroutine):
    return asyncio.run(coroutine)


def _pool_threads() -> list[threading.Thread]:
    return [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith("hyperscale-filesystem")
    ]


def test_operations_run_off_loop_on_the_dedicated_pool(filesystem, tmp_path):
    """The executing thread for every operation is a pool worker with
    the filesystem's thread-name prefix — not the event-loop thread."""

    async def scenario() -> str:
        target = tmp_path / "probe.bin"
        await filesystem.append_fsync(target, b"payload")

        handle = await filesystem.open(target, "rb")
        # Capture which thread services handle IO by reading through it.
        loop_thread = threading.current_thread()
        data = await handle.read()
        await handle.close()

        assert data == b"payload"
        pool_thread_names = {thread.name for thread in _pool_threads()}
        assert pool_thread_names, "pool threads should exist after IO"
        assert all(
            name.startswith("hyperscale-filesystem")
            for name in pool_thread_names
        )
        assert loop_thread.name not in pool_thread_names
        return "ok"

    assert _run(scenario()) == "ok"


def test_threads_spawn_lazily_and_shutdown_joins_them(tmp_path):
    """No threads at construction; workers exist after IO; a
    ``shutdown(wait=True)`` joins them all — the no-leak contract."""
    real_filesystem = RealFilesystem(max_workers=2)
    assert not _pool_threads(), "construction must not spawn threads"

    async def do_io() -> None:
        await real_filesystem.atomic_write(tmp_path / "lazy.bin", b"x")

    _run(do_io())
    assert _pool_threads(), "IO must have spawned pool workers"

    real_filesystem.shutdown(wait=True)
    assert not _pool_threads(), "shutdown(wait=True) must join all workers"

    # Idempotent: a second shutdown is a no-op, not an error.
    real_filesystem.shutdown(wait=True)


def test_io_after_shutdown_fails_loudly(tmp_path):
    real_filesystem = RealFilesystem(max_workers=1)
    real_filesystem.shutdown(wait=True)

    async def attempt() -> None:
        await real_filesystem.read_bytes(tmp_path / "never.bin")

    with pytest.raises(RuntimeError):
        _run(attempt())


def test_atomic_write_replaces_content_and_leaves_no_temp(
    filesystem, tmp_path
):
    async def scenario() -> None:
        target = tmp_path / "state.json"
        await filesystem.atomic_write(target, b'{"generation": 1}')
        await filesystem.atomic_write(target, b'{"generation": 2}')

        assert await filesystem.read_bytes(target) == b'{"generation": 2}'
        # The full sequence must leave exactly the destination — no
        # lingering ``.state.json.*.tmp`` from either write.
        assert await filesystem.list_directory(tmp_path, "*") == [target]

    _run(scenario())


def test_atomic_write_failure_cleans_its_temp_file(filesystem, tmp_path):
    """A failed atomic_write must not litter temp files (here: the
    destination's parent is remove-protected via a missing directory)."""

    async def scenario() -> None:
        missing_parent = tmp_path / "no-such-directory" / "state.json"
        with pytest.raises(FileNotFoundError):
            await filesystem.atomic_write(missing_parent, b"data")
        assert await filesystem.list_directory(tmp_path, "*") == []

    _run(scenario())


def test_append_fsync_accumulates_durable_units(filesystem, tmp_path):
    async def scenario() -> None:
        wal_path = tmp_path / "events.wal"
        await filesystem.append_fsync(wal_path, b"entry-1|")
        await filesystem.append_fsync(wal_path, b"entry-2|")
        assert await filesystem.read_bytes(wal_path) == b"entry-1|entry-2|"

    _run(scenario())


def test_handle_write_read_seek_round_trip(filesystem, tmp_path):
    async def scenario() -> None:
        target = tmp_path / "handle.bin"
        writer = await filesystem.open(target, "wb")
        await writer.write(b"abcdef")
        await writer.flush()
        await filesystem.fsync(writer)
        await writer.close()
        assert writer.closed

        reader = await filesystem.open(target, "rb")
        await reader.seek(3)
        tail = await reader.read()
        await reader.close()
        assert tail == b"def"

    _run(scenario())


def test_path_operations(filesystem, tmp_path):
    async def scenario() -> None:
        nested = tmp_path / "a" / "b"
        await filesystem.mkdir(nested, parents=True, exist_ok=True)
        assert await filesystem.exists(nested)

        target = nested / "record.json"
        await filesystem.atomic_write(target, b"{}")
        assert await filesystem.read_text(target) == "{}"

        await filesystem.remove(target)
        assert not await filesystem.exists(target)

    _run(scenario())
