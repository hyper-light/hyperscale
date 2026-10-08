"""
The manager's idempotency WAL stays as small as what it still holds.

Every keyed submission appends two entries -- its reservation and its
outcome -- and expiry dropped entries from memory only: the file grew for
the manager's whole life, and every start replayed all of it, re-indexing
entries long expired. The expiry sweep now rewrites the WAL to the live
entries once they fill no more than half of it (atomically: a crash keeps
the old file or the new one), and runs once at start, so what lapsed while
the manager was down goes at once.

A key's lapse also unmapped its job's key even when the job had since been
mapped to a newer one.

On virtual time, the SIM filesystem, and the real TaskRunner driving the
ledger's own cleanup loop:

* entries that lapsed are compacted out, and what is added after replays;
* a compaction keeps the live entries, which replay after a crash;
* a start compacts what lapsed while the manager was down;
* a key lapsing leaves its job mapped to the newer key;
* the ledger holds at most its configured entries, across a restart.
"""

import asyncio
import contextvars
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any, TypeVar

from hyperscale.distributed.idempotency.manager_ledger import IDEMPOTENCY_WAL_FORMAT
from hyperscale.distributed.idempotency.idempotency_config import IdempotencyConfig
from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKey
from hyperscale.distributed.idempotency.idempotency_status import IdempotencyStatus
from hyperscale.distributed.idempotency.manager_ledger import ManagerIdempotencyLedger
from hyperscale.distributed.runtime import restore_defaults, snapshot_defaults, swap_defaults
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import LoggingConfig
from tests.simulation.harness.sim import SimFilesystem, SimulationLoop, VirtualClock

ScenarioResult = TypeVar("ScenarioResult")

WAL_PATH = Path("/manager/idempotency.wal")
CONFIG = IdempotencyConfig()
# Past the committed TTL by two sweeps: every committed entry has lapsed
# and a sweep has run since.
PAST_COMMITTED_TTL_SECONDS = CONFIG.committed_ttl_seconds + 2 * CONFIG.cleanup_interval_seconds


class RecordingLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def log(self, model) -> None:
        self.messages.append(model.message)


def on_virtual_time(
    scenario: Callable[[SimFilesystem, TaskRunner], Coroutine[Any, Any, ScenarioResult]],
) -> ScenarioResult:
    """Run ``scenario`` on a fresh ``SimulationLoop`` with the process
    clock on its virtual time, with a SIM filesystem and a TaskRunner."""
    defaults = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock)

    async def run() -> ScenarioResult:
        task_runner = TaskRunner()
        try:
            return await scenario(SimFilesystem(clock=clock), task_runner)
        finally:
            await task_runner.shutdown()

    try:
        LoggingConfig().disable()
        return contextvars.copy_context().run(loop.run_until_complete, run())
    finally:
        LoggingConfig().enable()
        restore_defaults(defaults)
        loop.close()


async def open_ledger(filesystem: SimFilesystem, task_runner: TaskRunner) -> ManagerIdempotencyLedger:
    ledger = ManagerIdempotencyLedger(
        CONFIG, WAL_PATH, task_runner, RecordingLogger(), filesystem=filesystem
    )
    await ledger.start()
    return ledger


def key_of(sequence: int) -> IdempotencyKey:
    return IdempotencyKey(client_id="client-a", sequence=sequence, nonce="feedface")


async def accept(ledger: ManagerIdempotencyLedger, sequence: int, job_id: str) -> None:
    await ledger.check_or_reserve(key_of(sequence), job_id)
    await ledger.commit(key_of(sequence), f"ack-{sequence}".encode())


def test_lapsed_entries_are_compacted_out_and_what_follows_replays() -> None:
    async def scenario(filesystem: SimFilesystem, task_runner: TaskRunner):
        ledger = await open_ledger(filesystem, task_runner)
        for sequence in range(20):
            await accept(ledger, sequence, f"job-{sequence}")
        grown_size = await filesystem.file_size(WAL_PATH)

        await asyncio.sleep(PAST_COMMITTED_TTL_SECONDS)
        compacted_size = await filesystem.file_size(WAL_PATH)

        await accept(ledger, 20, "job-20")
        await ledger.close()
        filesystem.crash()
        recovered = await open_ledger(filesystem, task_runner)
        replayed = (recovered.get_by_key(key_of(20)), recovered.get_by_key(key_of(0)))
        await recovered.close()
        return grown_size, compacted_size, replayed

    grown_size, compacted_size, (kept, lapsed) = on_virtual_time(scenario)

    assert grown_size > 0
    # Nothing live: only the format header is left.
    assert compacted_size == IDEMPOTENCY_WAL_FORMAT.header_size
    assert kept is not None and (kept.status, kept.result_serialized) == (IdempotencyStatus.COMMITTED, b"ack-20")
    assert lapsed is None


def test_a_compaction_keeps_the_live_entries_through_a_crash() -> None:
    async def scenario(filesystem: SimFilesystem, task_runner: TaskRunner):
        ledger = await open_ledger(filesystem, task_runner)
        for sequence in range(20):
            await accept(ledger, sequence, f"job-{sequence}")
        # One entry accepted late enough to outlive the rest.
        await asyncio.sleep(CONFIG.committed_ttl_seconds - CONFIG.cleanup_interval_seconds)
        await accept(ledger, 20, "job-20")
        live_record_size = await filesystem.file_size(WAL_PATH)
        await asyncio.sleep(4 * CONFIG.cleanup_interval_seconds)
        compacted_size = await filesystem.file_size(WAL_PATH)

        await ledger.close()
        filesystem.crash()
        recovered = await open_ledger(filesystem, task_runner)
        replayed = recovered.get_by_key(key_of(20))
        await recovered.close()
        return live_record_size, compacted_size, replayed

    live_record_size, compacted_size, replayed = on_virtual_time(scenario)

    # The compacted WAL is the one committed entry: smaller than the file
    # that held 41 entries, larger than nothing.
    assert IDEMPOTENCY_WAL_FORMAT.header_size < compacted_size < live_record_size
    assert replayed is not None and replayed.status == IdempotencyStatus.COMMITTED


def test_a_start_compacts_what_lapsed_while_the_manager_was_down() -> None:
    async def scenario(filesystem: SimFilesystem, task_runner: TaskRunner):
        ledger = await open_ledger(filesystem, task_runner)
        for sequence in range(20):
            await accept(ledger, sequence, f"job-{sequence}")
        await ledger.close()

        await asyncio.sleep(PAST_COMMITTED_TTL_SECONDS)
        restarted = await open_ledger(filesystem, task_runner)
        state_at_start = (await filesystem.file_size(WAL_PATH), restarted.get_by_key(key_of(0)))
        await restarted.close()
        return state_at_start

    size_at_start, lapsed = on_virtual_time(scenario)

    assert (size_at_start, lapsed) == (IDEMPOTENCY_WAL_FORMAT.header_size, None)


def test_a_lapsing_key_leaves_its_job_mapped_to_the_newer_key() -> None:
    async def scenario(filesystem: SimFilesystem, task_runner: TaskRunner):
        ledger = await open_ledger(filesystem, task_runner)
        await accept(ledger, 1, "job-a")
        # The same job id under a newer key, accepted later: it outlives
        # the first.
        await asyncio.sleep(CONFIG.committed_ttl_seconds / 2)
        await accept(ledger, 2, "job-a")
        await asyncio.sleep(CONFIG.committed_ttl_seconds / 2 + 2 * CONFIG.cleanup_interval_seconds)
        mapped = ledger.get_by_job_id("job-a")
        first_key = ledger.get_by_key(key_of(1))
        await ledger.close()
        return mapped, first_key

    mapped, first_key = on_virtual_time(scenario)

    assert first_key is None
    assert mapped is not None and mapped.idempotency_key == key_of(2)


def test_the_ledger_holds_at_most_its_configured_entries_across_a_restart() -> None:
    """``IDEMPOTENCY_MAX_ENTRIES`` bounded the gate's cache and was ignored
    here: the oldest entries go first, as from the gate's cache, and the
    bound holds when a restart replays a WAL not yet compacted."""
    bounded_config = IdempotencyConfig(max_entries=3)

    async def scenario(filesystem: SimFilesystem, task_runner: TaskRunner):
        ledger = ManagerIdempotencyLedger(
            bounded_config, WAL_PATH, task_runner, RecordingLogger(), filesystem=filesystem
        )
        await ledger.start()
        for sequence in range(5):
            await accept(ledger, sequence, f"job-{sequence}")
        held = [ledger.get_by_key(key_of(sequence)) is not None for sequence in range(5)]
        mapped = [ledger.get_by_job_id(f"job-{sequence}") is not None for sequence in range(5)]
        await ledger.close()

        filesystem.crash()
        restarted = ManagerIdempotencyLedger(
            bounded_config, WAL_PATH, task_runner, RecordingLogger(), filesystem=filesystem
        )
        await restarted.start()
        held_after_restart = [restarted.get_by_key(key_of(sequence)) is not None for sequence in range(5)]
        await restarted.close()
        return held, mapped, held_after_restart

    held, mapped, held_after_restart = on_virtual_time(scenario)

    assert held == mapped == held_after_restart == [False, False, True, True, True]
