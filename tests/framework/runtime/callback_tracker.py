import asyncio


class CallbackTracker:
    def __init__(self) -> None:
        self.status_updates: list = []
        self.progress_updates: list = []
        self.workflow_results: dict = {}
        self.reporter_results: list = []
        self._lock = asyncio.Lock()

    async def on_status_update(self, push) -> None:
        async with self._lock:
            self.status_updates.append(push)

    async def on_progress_update(self, push) -> None:
        async with self._lock:
            self.progress_updates.append(push)

    async def on_workflow_result(self, push) -> None:
        async with self._lock:
            self.workflow_results[push.workflow_name] = push

    async def on_reporter_result(self, push) -> None:
        async with self._lock:
            self.reporter_results.append(push)

    def reset(self) -> None:
        self.status_updates.clear()
        self.progress_updates.clear()
        self.workflow_results.clear()
        self.reporter_results.clear()
