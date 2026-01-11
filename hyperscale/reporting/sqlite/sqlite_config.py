import os

from pydantic import BaseModel, ConfigDict, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class SQLiteConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    database_path: StrictStr = os.path.join(os.getcwd(), "results.db")
    workflow_results_table_name: StrictStr = "hyperscale_workflow_results"
    step_results_table_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.SQLite
