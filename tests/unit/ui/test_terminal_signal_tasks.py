"""
The terminal's SIGWINCH and SIGINT handlers hold the tasks they start
(R-G66).

The handlers were lambdas that called ``asyncio.create_task`` and dropped
the task: the loop keeps only a weak reference to a task, so a resize or
an abort could be collected mid-run, and stop()/abort() could not end a
resize that would then resume the render loop they had just stopped.

* every resize task is held until it ends, then released;
* stop()/abort() cancel and wait out the resizes still running;
* a SIGINT while the abort it started still runs starts no second abort;
  one after it ends does.
"""

import asyncio

import pytest

from hyperscale.ui.components.terminal import terminal as terminal_module
from hyperscale.ui.components.terminal.terminal import Terminal


def make_terminal() -> Terminal:
    terminal = Terminal([])
    terminal._loop = asyncio.get_running_loop()
    return terminal


@pytest.mark.asyncio
async def test_resize_tasks_are_held_until_they_end(monkeypatch: pytest.MonkeyPatch) -> None:
    resize_may_finish = asyncio.Event()

    async def resize_until_released(engine: Terminal) -> None:
        await resize_may_finish.wait()

    monkeypatch.setattr(terminal_module, "handle_resize", resize_until_released)
    terminal = make_terminal()

    terminal._on_resize_signal()
    terminal._on_resize_signal()
    held_tasks = set(terminal._resize_tasks)
    assert len(held_tasks) == 2

    resize_may_finish.set()
    await asyncio.gather(*held_tasks)
    await asyncio.sleep(0)

    assert terminal._resize_tasks == set()


@pytest.mark.asyncio
async def test_cancel_resize_tasks_ends_every_running_resize(monkeypatch: pytest.MonkeyPatch) -> None:
    never_released = asyncio.Event()

    async def resize_forever(engine: Terminal) -> None:
        await never_released.wait()

    monkeypatch.setattr(terminal_module, "handle_resize", resize_forever)
    terminal = make_terminal()
    terminal._on_resize_signal()
    terminal._on_resize_signal()
    held_tasks = set(terminal._resize_tasks)
    await asyncio.sleep(0)

    await terminal._cancel_resize_tasks()
    await asyncio.sleep(0)

    assert all(held_task.cancelled() for held_task in held_tasks)
    assert terminal._resize_tasks == set()


@pytest.mark.asyncio
async def test_a_repeated_interrupt_during_the_abort_starts_no_second_abort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    terminal = make_terminal()
    abort_may_finish = asyncio.Event()
    aborts_started: list[int] = []

    async def abort_until_released() -> None:
        aborts_started.append(len(aborts_started))
        await abort_may_finish.wait()

    monkeypatch.setattr(terminal, "_handle_keyboard_interrupt", abort_until_released)

    terminal._on_keyboard_interrupt_signal()
    first_abort = terminal._keyboard_interrupt_task
    terminal._on_keyboard_interrupt_signal()
    await asyncio.sleep(0)

    assert terminal._keyboard_interrupt_task is first_abort
    assert aborts_started == [0]

    abort_may_finish.set()
    await first_abort
    terminal._on_keyboard_interrupt_signal()
    await terminal._keyboard_interrupt_task

    assert terminal._keyboard_interrupt_task is not first_abort
    assert aborts_started == [0, 1]
