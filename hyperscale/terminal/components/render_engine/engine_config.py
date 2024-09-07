from pydantic import BaseModel, StrictFloat, StrictInt


class EngineConfig(BaseModel):
    width: StrictInt
    height: StrictInt
    refresh_rate = StrictInt | StrictFloat
