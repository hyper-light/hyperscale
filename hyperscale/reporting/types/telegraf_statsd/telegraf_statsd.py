import uuid
from typing import Dict, Literal, Callable


from hyperscale.core.results.workflow_types import MetricType
from hyperscale.reporting.types.common import ReporterTypes


try:
    from aio_statsd import TelegrafStatsdClient

    from hyperscale.reporting.types.statsd import StatsD

    from .teleraf_statsd_config import TelegrafStatsDConfig

    has_connector = True

except Exception:
    from hyperscale.reporting.types.empty import Empty as StatsD

    class TelegrafStatsDConfig:
        host: str = ""
        port: int = 0

    class TelegrafStatsdClient:
        pass

    has_connector = False


TelegrafStatsDMetricType = Literal[
    "increment",
    "histogram",
    "gauge",
    "timer",
    "distribution",
]


class TelegrafStatsD(StatsD):
    def __init__(self, config: TelegrafStatsDConfig) -> None:
        super().__init__(config)
        self.connection = TelegrafStatsdClient(host=config.host, port=config.port)

        self._types_map: Dict[
            MetricType,
            TelegrafStatsD,
        ] = {
            "COUNT": "increment",
            "DISTRIBUTION": "distribution",
            "RATE": "gauge",
            "TIMING": "timer",
            "SAMPLE": "gauge",
        }

        self._update_map: Dict[
            TelegrafStatsDMetricType,
            Callable[
                [str, int],
                None,
            ]
        ] = {
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

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.TelegrafStatsD
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

        self.statsd_type = "TelegrafStatsD"
