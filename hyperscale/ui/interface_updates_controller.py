import asyncio


class InterfaceUpdatesController:

    def __init__(self):
        self._update_lock = asyncio.Lock()
        self._active_workflows_updates: asyncio.Queue[list[str]] = asyncio.Queue()


    async def get_active_workflows(self):
        await self._update_lock.acquire()

        active_workflows_updates: list[str] | None = None

        if self._active_workflows_updates.empty() is False:
            active_workflows_updates = await self._active_workflows_updates.get()

        if self._update_lock.locked():
            self._update_lock.release()

        return active_workflows_updates
    
    async def update_active_workflows(
        self,
        workflows: list[str]
    ):
        await self._update_lock.acquire()
        self._active_workflows_updates.put(workflows)

        if self._update_lock.locked():
            self._update_lock.release()

    def shutdown(self):
        if self._update_lock.locked():
            self._update_lock.release()
    
