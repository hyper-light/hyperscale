from typing import Callable, Optional

try:

    from playwright.async_api import Worker

except Exception:
    class Worker:
        pass
    
from pydantic import BaseModel, ConfigDict, StrictBool, StrictFloat, StrictInt


class ExpectWorkerCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    predicate: Optional[Callable[[Worker], StrictBool]] = None
    timeout: StrictInt | StrictFloat
