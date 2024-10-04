from typing import Literal

from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr


class RateConfig(BaseModel):
    unit: StrictStr | None = None
    precision: StrictInt = 3
    rate_period: StrictInt | StrictFloat = 1
    rate_unit: Literal["s", "m", "h", "d"] = "s"

    def to_rate(self):
        return f"{self.rate_period}{self.rate_unit}"
