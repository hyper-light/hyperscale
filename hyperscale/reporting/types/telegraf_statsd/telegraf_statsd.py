import uuid
from typing import List


from hyperscale.reporting.metric import MetricType

try:
    from aio_statsd import TelegrafStatsdClient

    from hyperscale.reporting.types.statsd import StatsD

    from .teleraf_statsd_config import TelegrafStatsDConfig

    has_connector = True

except Exception:
    from hyperscale.reporting.types.empty import Empty as StatsD

    class TelegrafStatsDConfig:
        pass

    class TelegrafStatsdClient:
        pass

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
