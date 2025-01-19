from pydantic import BaseModel, StrictStr, StrictInt, StrictBool
from typing import Any, Dict

from hyperscale.reporting.common.types import ReporterTypes


class KafkaConfig(BaseModel):
    host: StrictStr = "localhost"
    port: StrictInt = 9092
    client_id: StrictStr = "hyperscale"
    workflow_results_topic_name: StrictStr = "hyperscale_workflow_results"
    step_results_topic_name: StrictStr = "hyperscale_step_results"
    workflow_results_partition: StrictInt = 0
    step_results_partition: StrictInt = 0
    compression_type: StrictStr | None = None
    timeout: StrictInt = 1000
    idempotent: StrictBool = True
    options: Dict[StrictStr, Any] = {}
    reporter_type: ReporterTypes = ReporterTypes.Kafka

    class Config:
        arbitrary_types_allowed = True
