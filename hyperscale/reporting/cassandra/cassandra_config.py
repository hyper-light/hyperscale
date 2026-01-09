from ssl import SSLContext
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, StrictStr, StrictInt

from hyperscale.reporting.common.types import ReporterTypes


class CassandraConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

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
