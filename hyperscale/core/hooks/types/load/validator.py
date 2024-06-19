from typing import Optional, Tuple, Union

from pydantic import BaseModel, StrictBool, StrictInt, StrictStr

from hyperscale.data.connectors.aws_lambda.aws_lambda_connector_config import (
    AWSLambdaConnectorConfig,
)
from hyperscale.data.connectors.bigtable.bigtable_connector_config import (
    BigTableConnectorConfig,
)


class LoadHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    loader: Union[
        AWSLambdaConnectorConfig,
        BigTableConnectorConfig,
    ]
    order: StrictInt
    skip: StrictBool

    class Config:
        arbitrary_types_allowed=True


