from typing import Dict, Iterator, List, TypeVar

from pydantic import BaseModel, ConfigDict, StrictBytes, StrictStr

from hyperscale.core.testing.models.base.base_types import (
    HTTPEncodableValue,
)

T = TypeVar("T")


class DataValidator(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: (
        StrictStr
        | StrictBytes
        | Iterator
        | Dict[HTTPEncodableValue, HTTPEncodableValue]
        | List[HTTPEncodableValue]
        | BaseModel
    )
