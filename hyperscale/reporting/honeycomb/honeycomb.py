import asyncio
import functools
import uuid
from hyperscale.reporting.common import (
    ReporterTypes,
    WorkflowMetricSet,
    StepMetricSet,
)

from .honeycomb_config import HoneycombConfig

try:
    import libhoney
    from libhoney import Event

    has_connector = True

except Exception:
    libhoney = object

    class Event:
        pass

    has_connector = False


class Honeycomb:
    def __init__(self, config: HoneycombConfig) -> None:
        self.api_key = config.api_key
        self._workflow_results_dataset_name = config.workflow_results_dataset_name
        self._step_results_dataset_name = config.step_results_dataset_name

        self._loop = asyncio.get_event_loop()
        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.Honeycomb
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

    async def connect(self):
        pass

    async def submit_workflow_results(
        self,
        workflow_results: WorkflowMetricSet,
    ):
        await self._loop.run_in_executor(
            None,
            functools.partial(
                libhoney.init,
                writekey=self.api_key,
                dataset=self._workflow_results_dataset_name,
                block_on_send=True,  # Consider this a safety measure since we don't want dropped events.
            ),
        )

        events: list[Event] = [
            libhoney.new_event(data=result) for result in workflow_results
        ]

        await asyncio.gather(
            *[self._loop.run_in_executor(None, event.send) for event in events]
        )

        await self._loop.run_in_executor(
            None,
            libhoney.flush,
        )

    async def submit_step_results(
        self,
        step_results: StepMetricSet,
    ):
        await self._loop.run_in_executor(
            None,
            functools.partial(
                libhoney.init,
                writekey=self.api_key,
                dataset=self._step_results_dataset_name,
                block_on_send=True,  # Consider this a safety measure since we don't want dropped events.
            ),
        )

        events: list[Event] = [
            libhoney.new_event(data=result) for result in step_results
        ]

        await asyncio.gather(
            *[self._loop.run_in_executor(None, event.send) for event in events]
        )

        await self._loop.run_in_executor(
            None,
            libhoney.flush,
        )

    async def close(self):
        await self._loop.run_in_executor(
            None,
            libhoney.close,
        )
