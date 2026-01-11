from pydantic import BaseModel, ConfigDict, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class GoogleCloudStorageConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    service_account_json_path: StrictStr
    bucket_namespace: StrictStr = "hyperscale"
    workflow_results_bucket_name: StrictStr = "hyperscale_workflow_results"
    step_results_bucket_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.GCS
