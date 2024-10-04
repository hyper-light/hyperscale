from pydantic import BaseModel, StrictInt, StrictStr


class CounterConfig(BaseModel):
    unit: StrictStr | None = None
    precision: StrictInt = 3
    initial_amount: StrictInt = 0
