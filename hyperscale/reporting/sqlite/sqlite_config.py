import os

from pydantic import BaseModel, StrictStr

from hyperscale.reporting.common.types import ReporterTypes


class SQLiteConfig(BaseModel):
    database_path: StrictStr = os.path.join(os.getcwd(), "results.db")
    workflow_results_table_name: StrictStr = "hyperscale_workflow_results"
    step_results_table_name: StrictStr = "hyperscale_step_results"
    reporter_type: ReporterTypes = ReporterTypes.SQLite

    class Config:
        arbitrary_types_allowed = True
