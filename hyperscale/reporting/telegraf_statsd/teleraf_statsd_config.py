from pydantic import BaseModel, StrictStr, StrictInt
from hyperscale.reporting.common.types import ReporterTypes


class TelegrafStatsDConfig(BaseModel):
    host: StrictStr = "0.0.0.0"
    port: StrictInt = 8125
    reporter_type: ReporterTypes = ReporterTypes.TelegrafStatsD

    class Config:
        arbitrary_types_allowed = True
