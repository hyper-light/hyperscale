"""
Incarnation persistence wiring — the restart contract at the
HealthAwareServer level.

The IncarnationStore existed and was seam-hardened but DOUBLY dormant:
no node passed ``incarnation_storage_dir`` and nothing called
``initialize_incarnation_store()``. All three nodes now wire it (the
manager derives the directory from ``wal_data_dir``), so these tests
pin the server-level semantics the wiring relies on, on a bare
instance (``object.__new__`` + the attributes the methods touch) over
one SimFilesystem across "process generations":

* first initialize seeds the tracker at the store's freshness
  bump (every initialize applies ``restart_incarnation_bump`` — even
  the first, so a node whose record was LOST still rejoins above any
  incarnation it plausibly used);
* refutation increments persist; a NEW server generation over the
  same directory initializes ABOVE the pre-restart value (the
  anti-zombie property);
* without a configured directory everything no-ops (incarnation 0,
  persist returns False) — persistence stays opt-in.
"""

from pathlib import Path

import pytest

from hyperscale.distributed.swim.detection.incarnation_store import (
    IncarnationStore,
)
from hyperscale.distributed.swim.detection.incarnation_tracker import (
    IncarnationTracker,
)
from hyperscale.distributed.swim.health_aware_server import HealthAwareServer
from tests.simulation.harness.sim import SimFilesystem


def _bare_server(
    storage_dir: str | None,
    filesystem: SimFilesystem,
) -> HealthAwareServer:
    server = object.__new__(HealthAwareServer)
    server._incarnation_storage_dir = storage_dir
    server._incarnation_store = None
    server._incarnation_tracker = IncarnationTracker()
    server._host = "127.0.0.1"
    server._udp_port = 9001
    server._udp_logger = None
    server._sim_filesystem = filesystem
    return server


async def _initialize_with_filesystem(
    server: HealthAwareServer,
) -> int:
    """Mirror ``initialize_incarnation_store`` exactly, but construct
    the store with the test's SimFilesystem injected up front — the
    server method binds the module-default (real) filesystem, which a
    unit test must not touch."""
    server._incarnation_store = IncarnationStore(
        storage_directory=Path(server._incarnation_storage_dir),
        node_address=f"{server._host}:{server._udp_port}",
        filesystem=server._sim_filesystem,
    )
    initial = await server._incarnation_store.initialize()
    server._incarnation_tracker.self_incarnation = initial
    return initial


@pytest.mark.asyncio
async def test_unconfigured_persistence_noops() -> None:
    filesystem = SimFilesystem()
    server = _bare_server(None, filesystem)

    assert await server.initialize_incarnation_store() == 0
    assert await server.persist_incarnation(5) is False


@pytest.mark.asyncio
async def test_restart_initializes_above_pre_restart_incarnation() -> None:
    filesystem = SimFilesystem()

    first_generation = _bare_server("/node/incarnation", filesystem)
    initial = await _initialize_with_filesystem(first_generation)
    freshness_bump = (
        first_generation._incarnation_store.restart_incarnation_bump
    )
    # Even a FRESH store starts at the freshness bump: a node whose
    # record was lost must still rejoin above anything it plausibly
    # used before.
    assert initial == freshness_bump

    # Two refutation-style increments, persisted like
    # HealthAwareServer.increment_incarnation does.
    bumped = initial
    for _ in range(2):
        bumped = await (
            first_generation._incarnation_tracker.increment_self_incarnation()
        )
        assert await first_generation.persist_incarnation(bumped)

    # New process generation over the SAME directory/disk.
    second_generation = _bare_server("/node/incarnation", filesystem)
    recovered = await _initialize_with_filesystem(second_generation)

    assert recovered > bumped, (
        "a restarted node must rejoin STRICTLY ABOVE its pre-restart "
        f"incarnation; persisted {bumped}, recovered {recovered}"
    )
    assert (
        second_generation._incarnation_tracker.get_self_incarnation()
        == recovered
    )


@pytest.mark.asyncio
async def test_persisted_incarnation_survives_power_loss() -> None:
    filesystem = SimFilesystem()

    first_generation = _bare_server("/node/incarnation", filesystem)
    await _initialize_with_filesystem(first_generation)
    bumped = await (
        first_generation._incarnation_tracker.increment_self_incarnation()
    )
    assert await first_generation.persist_incarnation(bumped)

    # Power loss, not clean shutdown: the store writes via
    # atomic_write, so the persisted value is durable by construction.
    filesystem.crash()

    second_generation = _bare_server("/node/incarnation", filesystem)
    recovered = await _initialize_with_filesystem(second_generation)
    assert recovered > bumped
