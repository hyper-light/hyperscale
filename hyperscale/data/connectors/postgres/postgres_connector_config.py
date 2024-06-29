from pydantic import BaseModel, StrictStr

from hyperscale.data.connectors.common.connector_type import ConnectorType


class PostgresConnectorConfig(BaseModel):
    host: StrictStr = "localhost"
    database: StrictStr
    username: StrictStr
    password: StrictStr
    table_name: StrictStr
    connector_type: ConnectorType = ConnectorType.Postgres

    class Config:
        arbitrary_types_allowed = True
