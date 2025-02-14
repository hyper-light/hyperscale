from typing import Callable, Optional

try:

    from playwright.async_api import Worker

except Exception:
    class Worker:
        pass
    
from pydantic import BaseModel, StrictBool, StrictFloat, StrictInt


class ExpectWorkerCommand(BaseModel):
    predicate: Optional[Callable[[Worker], StrictBool]] = None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True
