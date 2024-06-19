from pydantic import BaseModel, StrictStr

from hyperscale.data.connectors.common.connector_type import ConnectorType


class XMLConnectorConfig(BaseModel):
    filepath: StrictStr
    file_mode: StrictStr='r'
    reporter_type: ConnectorType=ConnectorType.XML
