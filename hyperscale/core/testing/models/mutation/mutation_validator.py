from typing import Dict

from pydantic import BaseModel, StrictStr

from hyperscale.core.testing.models.base.base_types import (
    HTTPEncodableValue,
)


class MutationValidator(BaseModel):
    query: StrictStr
    operation_name: StrictStr
    variables: Dict[StrictStr, HTTPEncodableValue]
