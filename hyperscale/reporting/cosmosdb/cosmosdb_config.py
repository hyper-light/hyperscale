from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class CosmosDBConfig(BaseModel):
    account_uri: StrictStr
    account_key: StrictStr
    database: StrictStr = "hyperscale"
    workflow_results_container_name: StrictStr = "hyperscale_workflow_results"
    step_results_container_name: StrictStr = "hyperscale_step_results"
    workflow_results_partition_key: StrictStr = "metric_workflow"
    step_results_partition_key: StrictStr = "metric_step"
    analytics_ttl: StrictInt = 0
    reporter_type: ReporterTypes = ReporterTypes.CosmosDB

    class Config:
        arbitrary_types_allowed = True
