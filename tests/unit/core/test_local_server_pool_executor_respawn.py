"""
REAL-mode executor death in LocalServerPool, with real spawned processes.

Each executor slot runs in a one-process pool of its own, so an executor
killed abruptly breaks only its own slot:

* only the killed slot is reported to the leader's exit listener;
* its siblings keep running, with the same processes;
* the slot is refilled by a new process, through the same spawn path;
* a pool shutting down replaces nothing, and every process is gone after.

The executors run ``hold_slot`` in place of ``run_thread``: a process that
lives until it is killed, with no server, so the test needs no network.
"""

import asyncio
import os
import signal
import time

import hyperscale.testing  # noqa: F401  (the engines' import order)
from hyperscale.core.jobs.models import Env
from hyperscale.core.jobs.runner import local_server_pool
from hyperscale.core.jobs.runner.local_server_pool import LocalServerPool

LEADER_ADDRESS = ("127.0.0.1", 41000)
SLOT_ADDRESSES = [("127.0.0.1", 41001), ("127.0.0.1", 41002), ("127.0.0.1", 41003)]
# Far past a spawn (interpreter start plus imports): a wait that reaches it
# is a hang.
HANG_SECONDS = 60.0
POLL_SECONDS = 0.05


def hold_slot(*args, **kwargs) -> None:
    """An executor that runs until it is killed."""
    time.sleep(HANG_SECONDS * 2)


def is_alive(process_id: int) -> bool:
    try:
        os.kill(process_id, 0)

    except ProcessLookupError:
        return False

    except PermissionError:
        return True

    return True


async def wait_until(condition) -> None:
    deadline = time.monotonic() + HANG_SECONDS
    while not condition():
        assert time.monotonic() < deadline, "timed out"
        await asyncio.sleep(POLL_SECONDS)


def live_process_ids(pool: LocalServerPool) -> set[int]:
    return {process_id for process_id, exitcode in pool.get_process_exitcodes().items() if exitcode is None}


def slot_process_ids(pool: LocalServerPool) -> dict[tuple[str, int], int]:
    return {
        address: next(iter(executor._processes))
        for address, executor in pool._executors.items()
        if executor._processes
    }


async def test_a_killed_executor_is_replaced_alone_and_shutdown_replaces_nothing(monkeypatch) -> None:
    exits: list[tuple[str, int]] = []
    pool = LocalServerPool(len(SLOT_ADDRESSES), on_executor_exit=exits.append)
    await pool.setup()
    monkeypatch.setattr(local_server_pool, "run_thread", hold_slot)

    try:
        await pool.run_pool(LEADER_ADDRESS, SLOT_ADDRESSES, Env())
        await wait_until(lambda: len(live_process_ids(pool)) == len(SLOT_ADDRESSES))
        before = slot_process_ids(pool)

        killed = SLOT_ADDRESSES[1]
        os.kill(before[killed], signal.SIGKILL)
        await wait_until(lambda: slot_process_ids(pool).get(killed, before[killed]) != before[killed])
        await wait_until(lambda: len(live_process_ids(pool)) == len(SLOT_ADDRESSES))
        after = slot_process_ids(pool)

        assert exits == [killed]
        assert {address: after[address] for address in after if address != killed} == {
            address: before[address] for address in before if address != killed
        }
        assert not is_alive(before[killed])
        assert is_alive(after[killed])

    finally:
        processes = set(slot_process_ids(pool).values())
        await pool.shutdown()

    await wait_until(lambda: not any(is_alive(process_id) for process_id in processes))
    assert pool._executors == {}
    # Each slot reported once more at shutdown; none was replaced.
    assert sorted(exits[1:]) == sorted(SLOT_ADDRESSES)
