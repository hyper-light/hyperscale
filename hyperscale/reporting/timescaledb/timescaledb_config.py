from pydantic import BaseModel, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class TimescaleDBConfig(BaseModel):
    host: StrictStr = "localhost"
    database: StrictStr = "hyperscale"
    username: StrictStr
    password: StrictStr
    workflow_results_table_name: StrictStr = "hyperscale_workflow_results"
    step_results_table_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.TimescaleDB

    class Config:
        arbitrary_types_allowed = True
