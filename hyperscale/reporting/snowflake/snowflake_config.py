from typing import Optional

from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class SnowflakeConfig(BaseModel):
    username: StrictStr
    password: StrictStr
    organization_id: StrictStr
    account_id: StrictStr
    private_key: StrictStr | None = None
    warehouse: StrictStr
    database: StrictStr = "hyperscale"
    database_schema: StrictStr = "PUBLIC"
    workflow_results_table_name: StrictStr = "hyperscale_workflow_results"
    step_results_table_name: StrictStr = "hyperscale_step_results"
    connect_timeout: int = 30
    reporter_type: ReporterTypes = ReporterTypes.Snowflake

    class Config:
        arbitrary_types_allowed = True
