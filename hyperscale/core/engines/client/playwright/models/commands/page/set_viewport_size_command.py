try:
    from playwright.async_api import ViewportSize

except Exception:
    class ViewportSize:
        pass
    
from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
)


class SetViewportSize(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    viewport_size: ViewportSize
    timeout: StrictInt | StrictFloat