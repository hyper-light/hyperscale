from pydantic import BaseModel, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class HoneycombConfig(BaseModel):
    api_key: StrictStr
    workflow_results_dataset_name: StrictStr = "hyperscale_workflow_results"
    step_results_dataset_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.Honeycomb

    class Config:
        arbitrary_types_allowed = True
