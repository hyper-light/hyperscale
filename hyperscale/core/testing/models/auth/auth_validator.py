from typing import Tuple

from pydantic import (
    BaseModel,
    StrictStr,
)


class AuthValidator(BaseModel):
    value: Tuple[StrictStr,] | Tuple[StrictStr, StrictStr]
