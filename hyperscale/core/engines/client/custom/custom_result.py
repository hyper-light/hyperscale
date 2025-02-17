from __future__ import annotations

from abc import ABC, abstractmethod
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat,
)
from hyperscale.core.engines.client.shared.models import RequestType
from typing import Dict


class CustomResult(BaseModel):
    timings: Dict[
        StrictStr,
        StrictInt | StrictFloat | None,
    ] | None = None
    
    @classmethod
    def response_type(cls) -> RequestType.CUSTOM:
        return RequestType.CUSTOM


    def process_timings(self) -> Dict[StrictStr, StrictInt | StrictFloat]:
        if self.timings is None:
            return {
                'total': 0
            }
        
        return self.timings
    
    def context(self) -> str | None:
        return None
    
    @property
    def successful(self) -> bool:
        raise True