from pydantic import BaseModel, StrictFloat, StrictInt
from .refresh_rate import RefreshRateProfile


class EngineConfig(BaseModel):
    width: StrictInt | None = None
    height: StrictInt | None = None
    refresh_profile: RefreshRateProfile = "medium"
    override_refresh_rate: StrictInt | StrictFloat | None = None
