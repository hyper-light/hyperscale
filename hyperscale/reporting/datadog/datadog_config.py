from typing import Dict

from pydantic import BaseModel, ConfigDict, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class DatadogConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    api_key: StrictStr
    app_key: StrictStr
    device_name: StrictStr = "hyperscale"
    priority: StrictStr = "normal"
    reporter_type: ReporterTypes = ReporterTypes.Datadog
