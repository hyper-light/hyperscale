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

        except Exception:
            pass

        except asyncio.CancelledError:
            pass

        if self._active_workflows_updates.empty() is False:
            active_workflows_updates = await self._active_workflows_updates.get()

        return active_workflows_updates

    def update_active_workflows(self, workflows: list[str]):
        self._active_workflows_updates.put_nowait(workflows)

        if not self._active_workflows_update_ready.is_set():
            self._active_workflows_update_ready.set()

    def shutdown(self):
        if not self._active_workflows_update_ready.is_set():
            self._active_workflows_update_ready.set()
