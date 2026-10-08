"""
B6 transient IO errors — ``SimFilesystem.set_io_error``.

The knob models transient device/controller failures: seeded draws
inside a VIRTUAL-time window raise ``OSError(5, "Input/output error")``
from the operation charge — reads and writes alike. The invariant the
wave-2 chaos suite builds on it: an EIO is retried or escalates LOUDLY,
never a silent skip. A failing operation has no effect — it lands no
bytes and consumes no disk-full budget (the write never reached the
platter).

Window tests drive virtual time through a ``SimulationLoop`` +
``VirtualClock`` (the same wiring every SIM child gets), and every
behavior is asserted deterministic: identical seeds produce the
identical raise pattern, operation by operation.
"""

import asyncio

import pytest

from tests.simulation.harness.sim import (
    SimFilesystem,
    SimulationLoop,
    VirtualClock,
)

_DATA_PATH = "/data/segment.bin"


def _run_virtual(coroutine_factory):
    """Run a scenario coroutine on a SimulationLoop with VirtualClock;
    returns the coroutine's result."""
    loop = SimulationLoop()
    asyncio.set_event_loop(loop)
    try:
        clock = VirtualClock(loop)
        return loop.run_until_complete(coroutine_factory(clock))
    finally:
        loop.close()
        asyncio.set_event_loop(None)


@pytest.mark.asyncio
async def test_io_error_always_raises_eio_when_unwindowed():
    filesystem = SimFilesystem()
    await filesystem.append_fsync(_DATA_PATH, b"pre-fault|")
    filesystem.set_io_error(seed=21, probability=1.0)

    with pytest.raises(OSError) as write_error:
        await filesystem.append_fsync(_DATA_PATH, b"never-lands|")
    assert write_error.value.errno == 5

    with pytest.raises(OSError) as read_error:
        await filesystem.read_bytes(_DATA_PATH)
    assert read_error.value.errno == 5

    # The failed write landed nothing; disarming restores the device.
    filesystem.clear_io_error()
    assert await filesystem.read_bytes(_DATA_PATH) == b"pre-fault|"


@pytest.mark.asyncio
async def test_failed_write_consumes_no_disk_full_budget():
    """EIO fires before budget accounting — the write never reached
    the platter, so the ENOSPC budget is intact afterwards."""
    filesystem = SimFilesystem()
    filesystem.set_disk_full(10)
    filesystem.set_io_error(seed=21, probability=1.0)

    with pytest.raises(OSError) as write_error:
        await filesystem.append_fsync(_DATA_PATH, b"12345")
    assert write_error.value.errno == 5

    filesystem.clear_io_error()
    # The full 10-byte budget must still fit this write.
    await filesystem.append_fsync(_DATA_PATH, b"1234567890")


def test_io_error_window_scopes_by_virtual_time():
    """Operations raise only inside ``[at_time, until_time)`` of the
    injected clock's virtual timeline."""

    async def scenario(clock: VirtualClock) -> None:
        filesystem = SimFilesystem(clock=clock)
        filesystem.set_io_error(
            seed=21, probability=1.0, at_time=5.0, until_time=10.0
        )

        await filesystem.append_fsync(_DATA_PATH, b"before-window|")

        await clock.sleep(6.0)  # virtual time 6.0 — inside the window
        with pytest.raises(OSError) as inside_error:
            await filesystem.append_fsync(_DATA_PATH, b"inside|")
        assert inside_error.value.errno == 5
        with pytest.raises(OSError):
            await filesystem.read_bytes(_DATA_PATH)

        await clock.sleep(5.0)  # virtual time 11.0 — past the window
        await filesystem.append_fsync(_DATA_PATH, b"after-window|")
        assert await filesystem.read_bytes(_DATA_PATH) == (
            b"before-window|after-window|"
        )

    _run_virtual(scenario)


@pytest.mark.asyncio
async def test_io_error_is_deterministic_per_seed():
    """Same seed: the identical raise pattern operation-by-operation.
    Different seed: a different pattern."""

    async def raise_pattern(seed: int) -> list[bool]:
        filesystem = SimFilesystem()
        await filesystem.append_fsync(_DATA_PATH, b"content|")
        filesystem.set_io_error(seed=seed, probability=0.5)
        pattern: list[bool] = []
        for _ in range(12):
            try:
                await filesystem.read_bytes(_DATA_PATH)
                pattern.append(False)
            except OSError as io_error:
                assert io_error.errno == 5
                pattern.append(True)
        return pattern

    first_pattern = await raise_pattern(21)
    second_pattern = await raise_pattern(21)
    other_seed_pattern = await raise_pattern(22)

    assert first_pattern == second_pattern
    assert first_pattern != other_seed_pattern
    # probability=0.5 over 12 ops: both outcomes occur (a fixed-seed
    # fact, stable forever under replay).
    assert any(first_pattern) and not all(first_pattern)


def test_windowed_io_error_without_clock_rejected():
    filesystem = SimFilesystem()
    with pytest.raises(ValueError):
        filesystem.set_io_error(seed=21, probability=0.5, at_time=5.0)


def test_io_error_validation():
    filesystem = SimFilesystem()
    with pytest.raises(ValueError):
        filesystem.set_io_error(seed=21, probability=1.5)
    with pytest.raises(ValueError):
        filesystem.set_io_error(seed=21, probability=-0.1)

    loop = SimulationLoop()
    try:
        clocked_filesystem = SimFilesystem(clock=VirtualClock(loop))
        with pytest.raises(ValueError):
            clocked_filesystem.set_io_error(
                seed=21, probability=0.5, at_time=10.0, until_time=5.0
            )
    finally:
        loop.close()
