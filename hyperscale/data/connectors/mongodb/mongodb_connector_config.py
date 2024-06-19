from typing import Optional

from pydantic import BaseModel, StrictStr

from hyperscale.data.connectors.common.connector_type import ConnectorType


class MongoDBConnectorConfig(BaseModel):
    host: StrictStr='localhost:27017'
    username: Optional[StrictStr]
    password: Optional[StrictStr]
    database: StrictStr
    collection: StrictStr
    connector_type: ConnectorType=ConnectorType.MongoDB