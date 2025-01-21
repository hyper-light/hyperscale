from typing import Dict

from pydantic import BaseModel, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class DatadogConfig(BaseModel):
    api_key: StrictStr
    app_key: StrictStr
    device_name: StrictStr = "hyperscale"
    priority: StrictStr = "normal"
    reporter_type: ReporterTypes = ReporterTypes.Datadog

    class Config:
        arbitrary_types_allowed = True
