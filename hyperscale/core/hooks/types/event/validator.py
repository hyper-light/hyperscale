from typing import Optional, Tuple
from pydantic import BaseModel, StrictStr, StrictInt, StrictBool


class EventHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    order: StrictInt
    skip: StrictBool
