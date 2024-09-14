from pydantic import BaseModel, StrictFloat, StrictInt


class EngineConfig(BaseModel):
    width: StrictInt | None = None
    height: StrictInt | None = None
    refresh_rate: StrictInt | StrictFloat = 80
