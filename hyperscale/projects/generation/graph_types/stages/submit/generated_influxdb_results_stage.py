import os

from hyperscale.core.graphs.stages import Submit
from hyperscale.reporting.types import InfluxDBConfig


class SubmitInfluxDBResultsStage(Submit):
    config=InfluxDBConfig(
        host='localhost:8006',
        token=os.getenv('INFLUXDB_API_TOKEN', ''),
        organization='<organization_here>',
        events_bucket='events',
        metrics_bucket='metrics'
    )