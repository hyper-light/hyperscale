import os

from pydantic import BaseModel, StrictStr, StrictBool

from hyperscale.reporting.common.types import ReporterTypes


class CSVConfig(BaseModel):
    workflow_results_filepath: StrictStr = os.path.join(
        os.getcwd(), "workflow_results.json"
    )
    step_results_filepath: StrictStr = os.path.join(os.getcwd(), "step_results.json")
    overwrite: StrictBool = True
    reporter_type: ReporterTypes = ReporterTypes.CSV

    class Config:
        arbitrary_types_allowed = True
