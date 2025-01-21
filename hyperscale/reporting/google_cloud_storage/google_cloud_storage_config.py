from pydantic import BaseModel, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class GoogleCloudStorageConfig(BaseModel):
    service_account_json_path: StrictStr
    bucket_namespace: StrictStr = "hyperscale"
    workflow_results_bucket_name: StrictStr = "hyperscale_workflow_results"
    step_results_bucket_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.GCS

    class Config:
        arbitrary_types_allowed = True
