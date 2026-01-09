from pydantic import BaseModel, ConfigDict, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class BigTableConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    service_account_json_path: StrictStr
    instance_id: StrictStr
    workflow_results_table_id: StrictStr = "hyperscale_workflow_results"
    step_results_table_id: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.BigTable
