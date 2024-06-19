from typing import Dict, Iterator, List

from pydantic import BaseModel, StrictBytes, StrictStr

from hyperscale.core_rewrite.hooks.optimized.models.base.base_types import (
    HTTPEncodableValue,
)


class DataValidator(BaseModel):
    value: (
        StrictStr
        | StrictBytes
        | Iterator
        | Dict[HTTPEncodableValue, HTTPEncodableValue]
        | List[HTTPEncodableValue]
        | BaseModel
    )

    class Config:
        arbitrary_types_allowed = True
