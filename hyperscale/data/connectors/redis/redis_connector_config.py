from typing import Optional

from pydantic import BaseModel, StrictBool, StrictInt, StrictStr

from hyperscale.data.connectors.common.connector_type import ConnectorType


class RedisConnectorConfig(BaseModel):
    host: StrictStr = "localhost:6379"
    username: Optional[StrictStr]
    password: Optional[StrictStr]
    database: StrictInt = 0
    channel: StrictStr
    channel_type: StrictStr = "pipeline"
    secure: StrictBool = False
    connector_type: ConnectorType = ConnectorType.Redis
