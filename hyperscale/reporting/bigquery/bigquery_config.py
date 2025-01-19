from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class BigQueryConfig(BaseModel):
    service_account_json_path: str
    project_name: StrictStr
    dataset_name: StrictStr = "hyperscale"
    dataset_location: StrictStr = "US"
    workflow_results_table_name: StrictStr = "hyperscale_workflow_results"
    step_results_table_name: StrictStr = "hyperscale_step_results"
    retry_timeout: StrictInt = 30
    reporter_type: ReporterTypes = ReporterTypes.BigQuery

    class Config:
        arbitrary_types_allowed = True
