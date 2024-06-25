from typing import Dict

from pydantic import (
    BaseModel,
    StrictStr,
)

from hyperscale.core_rewrite.testing.models.base.base_types import (
    HTTPEncodableValue,
)


class HeaderValidator(BaseModel):
    value: Dict[StrictStr, HTTPEncodableValue]
