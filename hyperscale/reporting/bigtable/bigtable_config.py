from pydantic import BaseModel, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class BigTableConfig(BaseModel):
    service_account_json_path: StrictStr
    instance_id: StrictStr
    workflow_results_table_id: StrictStr = "hyperscale_workflow_results"
    step_results_table_id: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.BigTable

    class Config:
        arbitrary_types_allowed = True
