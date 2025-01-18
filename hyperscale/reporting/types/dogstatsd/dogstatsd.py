import uuid
from typing import Dict, Literal, Callable


from hyperscale.core.results.workflow_types import MetricType
from hyperscale.reporting.types.common import ReporterTypes

try:
    from aio_statsd import DogStatsdClient

    from hyperscale.reporting.types.statsd import StatsD

    from .dogstatsd_config import DogStatsDConfig

    has_connector = True

except Exception:
    from hyperscale.reporting.types.empty import Empty as StatsD

    has_connector = False

    class DogStatsDConfig:
        pass

    class DogStatsdClient:
        pass


DogStatsDMetricType = Literal[
    "increment",
    "histogram",
    "gauge",
    "timer",
    "distribution",
]


class DogStatsD(StatsD):
    def __init__(self, config: DogStatsDConfig) -> None:
        super(DogStatsD, self).__init__(config)

        self.host = config.host
        self.port = config.port

        self.connection = DogStatsdClient(host=self.host, port=self.port)

        self._types_map: Dict[
            MetricType,
            DogStatsDMetricType,
        ] = {
            "COUNT": "increment",
            "DISTRIBUTION": "distribution",
            "RATE": "gauge",
            "TIMING": "timer",
            "SAMPLE": "gauge",
        }


        self._update_map: Dict[
            DogStatsDMetricType,
            Callable[
                [str, int],
                None,
            ]
        ] = {
            "count": lambda: NotImplementedError("DogStatsD does not support counts."),
            "gauge": self.connection.gauge,
            "sets": lambda: NotImplementedError("DogStatsD does not support sets."),
            "increment": self.connection.increment,
            "histogram": self.connection.histogram,
            "distribution": self.connection.distribution,
            "timer": self.connection.timer,
        }

        self.session_uuid = str(uuid.uuid4())
        self.reporter_type = ReporterTypes.DogStatsD
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

        self.statsd_type = "StatsD"
