from pydantic import BaseModel, ConfigDict, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class S3Config(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    region_name: StrictStr
    workflow_results_bucket_name: StrictStr = "hyperscale_workflow_results"
    step_results_bucket_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.S3
