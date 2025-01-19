from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class MySQLConfig(BaseModel):
    host: StrictStr = "localhost"
    port: StrictInt = 3306
    database: StrictStr = "hyperscale"
    username: StrictStr
    password: StrictStr
    worfklow_results_table_name: StrictStr = "hyperscale_workflow_results"
    step_results_table_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.MySQL

    class Config:
        arbitrary_types_allowed = True
