from typing import Dict

from pydantic import (
    BaseModel,
    StrictStr,
)

from hyperscale.core_rewrite.optimized.models.base.base_types import (
    HTTPEncodableValue,
)


class ParamsValidator(BaseModel):
    value: Dict[StrictStr, HTTPEncodableValue]