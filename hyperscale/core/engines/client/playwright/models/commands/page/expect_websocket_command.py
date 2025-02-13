from typing import Callable, Optional

try:
    from playwright.async_api import WebSocket

except Exception:
    class WebSocket:
        pass
    
from pydantic import BaseModel, StrictBool, StrictFloat, StrictInt


class ExpectWebsocketCommand(BaseModel):
    predicate: Optional[Callable[[WebSocket], StrictBool]] = None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True
