from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class GraphiteConfig(BaseModel):
    host: StrictStr = "localhost"
    port: StrictInt = 2003
    reporter_type: ReporterTypes = ReporterTypes.Graphite

    class Config:
        arbitrary_types_allowed = True
