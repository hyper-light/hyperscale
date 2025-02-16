from pydantic import BaseModel, StrictStr
from typing import Dict

from typing import Any


class CustomResponse(BaseModel):
    timings: Dict[
        str,
        float | None,
    ] | None = None

    @classmethod
    def response_type(cls) -> Any:
        return 'Custom'
    
    def process_timings(self):
        timings = self.timings
        if timings is None:
            timings = {
                'total': 0
            }

        return timings
    
    def context(self):
        return None

    @property
    def successful(self) -> bool:
        return True