import asyncio


async def return_if_safe(task: asyncio.Task):
    if task and not task.cancelled():
        return await task
