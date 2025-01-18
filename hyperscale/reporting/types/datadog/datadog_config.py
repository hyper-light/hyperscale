from typing import Dict

from pydantic import BaseModel, StrictStr

from hyperscale.reporting.types.common.types import ReporterTypes


class DatadogConfig(BaseModel):
    api_key: StrictStr
    app_key: StrictStr
    device_name: StrictStr = "hyperscale"
    priority: StrictStr = "normal"
    custom_fields: Dict[StrictStr, StrictStr] = {}
    reporter_type: ReporterTypes = ReporterTypes.Datadog

    class Config:
        arbitrary_types_allowed = True