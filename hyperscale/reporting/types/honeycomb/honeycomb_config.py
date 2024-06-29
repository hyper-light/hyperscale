from pydantic import BaseModel

from hyperscale.reporting.types.common.types import ReporterTypes


class HoneycombConfig(BaseModel):
    api_key: str
    dataset: str
    reporter_type: ReporterTypes = ReporterTypes.Honeycomb
