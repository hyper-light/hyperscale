from pydantic import BaseModel, ConfigDict, StrictStr, StrictInt, StrictBool

from hyperscale.reporting.common.types import ReporterTypes


class InfluxDBConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: StrictStr = "localhost"
    port: StrictInt = 8086
    token: StrictStr
    organization: StrictStr
    connect_timeout: StrictInt = 10000
    workflow_results_bucket_name: StrictStr = "hyperscale_workflow_results"
    step_results_bucket_name: StrictStr = "hyperscale_step_results"
    secure: StrictBool = False
    reporter_type: ReporterTypes = ReporterTypes.InfluxDB
