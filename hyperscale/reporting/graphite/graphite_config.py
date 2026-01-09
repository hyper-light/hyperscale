from pydantic import BaseModel, ConfigDict, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class GraphiteConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: StrictStr = "localhost"
    port: StrictInt = 2003
    reporter_type: ReporterTypes = ReporterTypes.Graphite
