from typing import Optional

from pydantic import BaseModel

from hyperscale.reporting.types.common.types import ReporterTypes


class MongoDBConfig(BaseModel):
    host: str = "localhost:27017"
    username: Optional[str]
    password: Optional[str]
    database: str = "hyperscale"
    events_collection: str = "events"
    metrics_collection: str = "metrics"
    experiments_collection: str = "experiment"
    streams_collection: str = "streams"
    system_metrics_collection: str = "system_metrics"
    reporter_type: ReporterTypes = ReporterTypes.MongoDB
