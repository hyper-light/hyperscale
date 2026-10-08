import asyncio


async def cancel(task: asyncio.Task):
    if task is None:
        return

    try:
        if task.done():
            task.exception()

            return task

        task.cancel()
        await asyncio.sleep(0)
        if not task.cancelled():
            await task

        return task

    except asyncio.CancelledError:
        # The cancelled task's own CancelledError ends here; one
        # cancelling the caller -- the task running this -- goes on.
        if asyncio.current_task().cancelling():
            raise

    except asyncio.InvalidStateError:
        pass
