import asyncio


class InterfaceUpdatesController:
    def __init__(self):
        self._active_workflows_updates: asyncio.Queue[list[str]] = asyncio.Queue()
        self._active_workflows_update_ready = asyncio.Event()

    async def get_active_workflows(
        self,
        timeout: int,
    ):
        active_workflows_updates: list[str] | None = None

        try:
            await asyncio.wait_for(
                self._active_workflows_update_ready.wait(), timeout=timeout
            )

        except TimeoutError:
            # No update within the timeout: the caller keeps cycling the
            # workflows it has. Anything else, cancellation included, is
            # the caller's to see.
            pass

        if self._active_workflows_updates.empty() is False:
            active_workflows_updates = await self._active_workflows_updates.get()

        # Every queued update is taken: the next call waits for a new one
        # (update_active_workflows sets the event again) or for the timeout,
        # instead of returning at once from then on.
        if self._active_workflows_updates.empty():
            self._active_workflows_update_ready.clear()

        return active_workflows_updates

    def update_active_workflows(self, workflows: list[str]):
        self._active_workflows_updates.put_nowait(workflows)

        if not self._active_workflows_update_ready.is_set():
            self._active_workflows_update_ready.set()

    def shutdown(self):
        if not self._active_workflows_update_ready.is_set():
            self._active_workflows_update_ready.set()

        # Drain the queue to release any held references
        while not self._active_workflows_updates.empty():
            try:
                self._active_workflows_updates.get_nowait()
            except asyncio.QueueEmpty:
                break
