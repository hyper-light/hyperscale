from typing import Literal

from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr


class WindowedRateConfig(BaseModel):
    unit: StrictStr | None = None
    precision: StrictInt = 3
    rate_period: StrictInt | StrictFloat = 1
    rate_unit: Literal["s", "m", "h", "d"] = "s"
