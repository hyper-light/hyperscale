try:
    from playwright.async_api import ViewportSize

except Exception:
    class ViewportSize:
        pass
    
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class SetViewportSize(BaseModel):
    viewport_size: ViewportSize
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True