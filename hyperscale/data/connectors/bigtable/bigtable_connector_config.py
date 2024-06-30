from pydantic import BaseModel, StrictStr

from hyperscale.data.connectors.common.connector_type import ConnectorType


class BigTableConnectorConfig(BaseModel):
    service_account_json_path: StrictStr
    instance_id: StrictStr
    table_name: StrictStr
    connector_type: ConnectorType = ConnectorType.BigTable
