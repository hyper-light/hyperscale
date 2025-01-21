from typing import Dict

from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class AWSTimestreamConfig(BaseModel):
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    region_name: StrictStr
    database_name: StrictStr = "hyperscale"
    workflow_results_table_name: StrictStr = "hyperscale_workflow_results"
    step_results_table_name: StrictStr = "hyperscale_step_results"
    retention_options: Dict[StrictStr, StrictInt] = {
        "MemoryStoreRetentionPeriodInHours": 1,
        "MagneticStoreRetentionPeriodInDays": 365,
    }
    reporter_type: ReporterTypes = ReporterTypes.AWSTimestream
