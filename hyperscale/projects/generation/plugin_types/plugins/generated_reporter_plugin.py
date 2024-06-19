from typing import List

from hyperscale.plugins.types.common import Event
from hyperscale.plugins.types.reporter import (
    Metrics,
    ReporterConfig,
    ReporterPlugin,
    process_custom,
    process_errors,
    process_events,
    process_metrics,
    process_shared,
    reporter_close,
    reporter_connect,
)


class CustomReporterConfig(ReporterConfig):
    pass


class  CustomReporter(ReporterPlugin):
    config=CustomReporterConfig

    def __init__(self, config: CustomReporterConfig) -> None:
        super().__init__(config)

    @reporter_connect()
    async def reporter_connect(self):
        pass

    @process_events()
    async def reporter_process_events(self, events: List[Event]):
        pass

    @process_shared()
    async def reporter_process_shared_metrics(self, metrics: List[Metrics]):
        pass

    @process_metrics()
    async def reporter_process_metrics(self, metrics: List[Metrics]):
        pass

    @process_custom()
    async def reporter_process_custom_metrics(self, metrics: List[Metrics]):
        pass

    @process_errors()
    async def reporter_process_errors(self, metrics: List[Metrics]):
        pass

    @reporter_close()
    async def reporter_close(self):
        pass