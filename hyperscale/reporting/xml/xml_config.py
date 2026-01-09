import os

from pydantic import BaseModel, ConfigDict, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class XMLConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    workflow_results_filepath: StrictStr = os.path.join(
        os.getcwd(),
        "workflow_results.xml",
    )
    step_results_filepath: StrictStr = os.path.join(
        os.getcwd(),
        "step_results.xml",
    )
    reporter_type: ReporterTypes = ReporterTypes.XML
