from pydantic import BaseModel, ConfigDict, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class HoneycombConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    api_key: StrictStr
    workflow_results_dataset_name: StrictStr = "hyperscale_workflow_results"
    step_results_dataset_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.Honeycomb
