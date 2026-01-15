from typing import Callable, Optional

try:
    from playwright.async_api import WebSocket

except Exception:
    class WebSocket:
        pass
    
from pydantic import BaseModel, ConfigDict, StrictBool, StrictFloat, StrictInt


class ExpectWebsocketCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    predicate: Optional[Callable[[WebSocket], StrictBool]] = None
    timeout: StrictInt | StrictFloat
