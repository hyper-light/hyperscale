from typing import Dict, Iterator, List, TypeVar

from pydantic import BaseModel, StrictBytes, StrictStr

from hyperscale.core.testing.models.base.base_types import (
    HTTPEncodableValue,
)

T = TypeVar("T")


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
