from pydantic import BaseModel, StrictStr

from hyperscale.data.connectors.common.connector_type import ConnectorType


class AWSLambdaConnectorConfig(BaseModel):
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    region_name: StrictStr
    lambda_name: StrictStr = "actions"
    connector_type: ConnectorType = ConnectorType.AWSLambda
