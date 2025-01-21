from ssl import SSLContext
from typing import List, Optional

from pydantic import BaseModel, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class CassandraConfig(BaseModel):
    hosts: List[StrictStr] = ["127.0.0.1"]
    port: StrictInt = 9042
    username: StrictStr | None = None
    password: StrictStr | None = None
    keyspace: StrictStr = "hyperscale"
    workflow_results_table_name: StrictStr = "hyperscale_workflow_results"
    step_results_table_name: StrictStr = "hyperscale_step_results"
    system_metrics_table: StrictStr = "system_metrics"
    replication_strategy: StrictStr = "SimpleStrategy"
    replication: StrictInt = 3
    ssl: Optional[SSLContext] = None
    reporter_type: ReporterTypes = ReporterTypes.Cassandra

    class Config:
        arbitrary_types_allowed = True
