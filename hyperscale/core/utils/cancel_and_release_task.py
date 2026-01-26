import asyncio


def _retrieve_task_exception(task: asyncio.Task) -> None:
    """
    Done callback to retrieve a task's exception and prevent memory leaks.

    Python's asyncio keeps task objects alive if their exception is never
    retrieved. This callback ensures exceptions are always retrieved.
    """
    try:
        task.exception()
    except (asyncio.CancelledError, asyncio.InvalidStateError, Exception):
        pass


def cancel_and_release_task(pend: asyncio.Task) -> None:
    """
    Cancel a task and guarantee no memory leaks, even for hung tasks.

    This handles both done and running tasks:
    - Done tasks: retrieve exception immediately
    - Running tasks: cancel + add done callback to retrieve exception later

    The done callback is critical: even if a task is stuck in a syscall
    (SSL, network), when it eventually finishes, the callback fires and
    retrieves the exception, allowing GC to clean up.

    Args:
        pend: The asyncio.Task to cancel
    """
    try:
        if pend.done():
            # Task already finished - retrieve exception now
            try:
                pend.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError, Exception):
                pass
        else:
            # Task still running - cancel and add callback for when it finishes
            # The callback ensures exception is retrieved even if task is stuck
            pend.add_done_callback(_retrieve_task_exception)
            pend.cancel()
    except Exception:
        pass