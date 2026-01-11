from pydantic import BaseModel, ConfigDict, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class TelegrafStatsDConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: StrictStr = "0.0.0.0"
    port: StrictInt = 8125
    reporter_type: ReporterTypes = ReporterTypes.TelegrafStatsD
