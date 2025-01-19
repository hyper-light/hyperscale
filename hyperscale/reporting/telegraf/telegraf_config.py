from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class TelegrafConfig(BaseModel):
    host: StrictStr = "localhost"
    port: StrictInt = 8094
    reporter_type: ReporterTypes = ReporterTypes.Telegraf
