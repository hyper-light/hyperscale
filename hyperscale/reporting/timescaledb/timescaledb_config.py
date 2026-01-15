from pydantic import BaseModel, ConfigDict, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class TimescaleDBConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: StrictStr = "localhost"
    database: StrictStr = "hyperscale"
    username: StrictStr
    password: StrictStr
    workflow_results_table_name: StrictStr = "hyperscale_workflow_results"
    step_results_table_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.TimescaleDB
