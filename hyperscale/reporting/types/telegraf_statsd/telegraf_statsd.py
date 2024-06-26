import uuid
from typing import List

from hyperscale.logging.hyperscale_logger import HyperscaleLogger
from hyperscale.reporting.metric import MetricType
from hyperscale.reporting.processed_result.types.base_processed_result import (
    BaseProcessedResult,
)

try:
    from aio_statsd import TelegrafStatsdClient

    from hyperscale.reporting.types.statsd import StatsD

    from .teleraf_statsd_config import TelegrafStatsDConfig

    has_connector = True

except Exception:
    from hyperscale.reporting.types.empty import Empty as StatsD

    TelegrafStatsDConfig = None
    TelegrafStatsdClient = None
    has_connector = False


class TelegrafStatsD(StatsD):
    def __init__(self, config: TelegrafStatsDConfig) -> None:
        super().__init__(config)
        self.connection = TelegrafStatsdClient(host=self.host, port=self.port)

        self.types_map = {
            "total": "increment",
            "succeeded": "increment",
            "failed": "increment",
            "median": "gauge",
            "mean": "gauge",
            "variance": "gauge",
            "stdev": "gauge",
            "minimum": "gauge",
            "maximum": "gauge",
            "quantiles": "gauge",
        }

        self._update_map = {
            "count": lambda: NotImplementedError(
                "TelegrafStatsD does not support counts."
            ),
            "gauge": self.connection.gauge,
            "sets": lambda: NotImplementedError(
                "TelegrafStatsD does not support sets."
            ),
            "increment": self.connection.increment,
            "histogram": self.connection.histogram,
            "distribution": self.connection.distribution,
            "timer": self.connection.timer,
        }

        self.stat_type_map = {
            MetricType.COUNT: "increment",
            MetricType.DISTRIBUTION: "gauge",
            MetricType.RATE: "gauge",
            MetricType.SAMPLE: "gauge",
        }

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.statsd_type = "TelegrafStatsD"

    async def submit_events(self, events: List[BaseProcessedResult]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitting Events to {self.statsd_type}"
        )

        for event in events:
            time_update_function = self._update_map.get("gauge")
            time_update_function(f"{event.name}_time", event.time)

            if event.success:
                success_update_function = self._update_map.get("increment")
                success_update_function(f"{event.name}_success", 1)

            else:
                failed_update_function = self._update_map.get("increment")
                failed_update_function(f"{event.name}_failed", 1)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Submitted Events to {self.statsd_type}"
        )
