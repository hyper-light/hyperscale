from pydantic import BaseModel, StrictStr

from hyperscale.reporting.types.common.types import ReporterTypes


class HoneycombConfig(BaseModel):
    api_key: StrictStr
    dataset: StrictStr = 'hyperscale'
    reporter_type: ReporterTypes = ReporterTypes.Honeycomb

    class Config:
        arbitrary_types_allowed = True