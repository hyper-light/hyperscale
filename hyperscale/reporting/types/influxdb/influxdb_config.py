from pydantic import BaseModel

from hyperscale.reporting.types.common.types import ReporterTypes


class InfluxDBConfig(BaseModel):
    host: str = "localhost:8086"
    token: str
    organization: str = "hyperscale"
    connect_timeout: int = 10000
    events_bucket: str = "events"
    metrics_bucket: str = "metrics"
    experiments_bucket: str = "experiments"
    streams_bucket: str = "streams"
    system_metrics_bucket: str = "system_metrics"
    secure: bool = False
    reporter_type: ReporterTypes = ReporterTypes.InfluxDB
