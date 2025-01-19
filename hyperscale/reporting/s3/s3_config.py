from pydantic import BaseModel, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class S3Config(BaseModel):
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    region_name: StrictStr
    workflow_results_bucket_name: StrictStr = "hyperscale_workflow_results"
    step_results_bucket_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.S3

    class Config:
        arbitrary_types_allowed = True
