from typing import Dict

from pydantic import (
    BaseModel,
    StrictStr,
)

from hyperscale.core_rewrite.hooks.optimized.models.base.base_types import (
    HTTPEncodableValue,
)


class HeaderValidator(BaseModel):
    value: Dict[StrictStr, HTTPEncodableValue]
