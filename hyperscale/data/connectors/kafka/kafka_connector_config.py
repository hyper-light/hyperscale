from typing import Any, Dict, Optional

from pydantic import BaseModel, StrictBool, StrictInt, StrictStr

from hyperscale.data.connectors.common.connector_type import ConnectorType


class KafkaConnectorConfig(BaseModel):
    host: StrictStr='localhost:9092'
    client_id: StrictStr='hyperscale'
    topic: StrictStr
    partition: StrictInt=0
    compression_type: Optional[StrictStr]
    timeout: StrictInt=1000
    idempotent: StrictBool=True
    options: Dict[StrictInt, Any]={}
    reporter_type: ConnectorType=ConnectorType.Kafka