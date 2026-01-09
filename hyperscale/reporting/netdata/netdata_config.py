from pydantic import BaseModel, ConfigDict, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class NetdataConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: StrictStr = "localhost"
    port: StrictInt = 8125
    reporter_type: ReporterTypes = ReporterTypes.Netdata
