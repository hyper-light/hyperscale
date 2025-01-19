from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class StatsDConfig(BaseModel):
    host: StrictStr = "localhost"
    port: StrictInt = 8125
    reporter_type: ReporterTypes = ReporterTypes.StatsD

    class Config:
        arbitrary_types_allowed = True
