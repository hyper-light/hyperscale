from pydantic import BaseModel, StrictInt, StrictStr


class TotalRateConfig(BaseModel):
    unit: StrictStr | None = None
    precision: StrictInt = 3
