"""
IncarnationStore: persistence semantics + Phase 7 storage seam.

The store exists to prevent zombie-node split-brain: a restarted node
must rejoin with an incarnation strictly above anything it previously
used. Pins:

* restart round-trip: a fresh store on the same path resumes ABOVE the
  persisted value (the restart bump);
* update monotonicity: lower-or-equal incarnations are rejected;
* every disk touch flows through the injected Filesystem — saves are
  ``atomic_write`` (the full fsync'd crash-consistency sequence; the
  previous inline temp-then-rename skipped every fsync AND ran
  synchronously on the event loop).
"""

from pathlib import Path

import pytest

from hyperscale.core.runtime import RealFilesystem
from hyperscale.distributed.swim.detection.incarnation_store import (
    IncarnationStore,
)


@pytest.fixture
def filesystem():
    real_filesystem = RealFilesystem(max_workers=2)
    yield real_filesystem
    real_filesystem.shutdown(wait=True)


def _store(tmp_path, filesystem) -> IncarnationStore:
    return IncarnationStore(
        storage_directory=Path(tmp_path) / "incarnations",
        node_address="127.0.0.1:9001",
        filesystem=filesystem,
    )


@pytest.mark.asyncio
async def test_restart_resumes_above_persisted_incarnation(
    tmp_path, filesystem
):
    store = _store(tmp_path, filesystem)
    first_incarnation = await store.initialize()
    assert first_incarnation == store.restart_incarnation_bump

    assert await store.update_incarnation(first_incarnation + 5) is True

    restarted = _store(tmp_path, filesystem)
    resumed_incarnation = await restarted.initialize()
    # Strictly above everything the previous process used — the
    # zombie-prevention guarantee.
    assert resumed_incarnation == (
        first_incarnation + 5 + restarted.restart_incarnation_bump
    )


@pytest.mark.asyncio
async def test_update_incarnation_is_monotone(tmp_path, filesystem):
    store = _store(tmp_path, filesystem)
    initial = await store.initialize()

    assert await store.update_incarnation(initial + 1) is True
    assert await store.update_incarnation(initial + 1) is False
    assert await store.update_incarnation(initial) is False
    assert await store.get_incarnation() == initial + 1


@pytest.mark.asyncio
async def test_saves_flow_through_atomic_write_on_the_seam(tmp_path):
    """Every save is one crash-safe atomic_write on the seam — and no
    real disk IO happens when a fake is injected."""

    class RecordingFilesystem:
        def __init__(self) -> None:
            self.atomic_writes: list[tuple[Path, bytes]] = []

        async def mkdir(self, path, *, parents=False, exist_ok=False):
            return None

        async def exists(self, path) -> bool:
            return False

        async def atomic_write(self, path, data: bytes) -> None:
            self.atomic_writes.append((Path(path), bytes(data)))

    recording = RecordingFilesystem()
    store = IncarnationStore(
        storage_directory=Path(tmp_path) / "never-created",
        node_address="127.0.0.1:9001",
        filesystem=recording,
    )
    initial = await store.initialize()
    await store.update_incarnation(initial + 3)

    assert len(recording.atomic_writes) == 2  # initialize + update
    assert all(
        path.name == "incarnation_127.0.0.1_9001.json"
        for path, _data in recording.atomic_writes
    )
    assert not (Path(tmp_path) / "never-created").exists()
