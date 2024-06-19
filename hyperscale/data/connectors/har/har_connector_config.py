from pydantic import BaseModel, StrictStr

from hyperscale.data.connectors.common.connector_type import ConnectorType


class HARConnectorConfig(BaseModel):
    filepath: StrictStr
    connector_type: ConnectorType=ConnectorType.CSV