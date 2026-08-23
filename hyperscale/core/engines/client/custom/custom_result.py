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
        """Whether this result counts as a success.

        Subclasses MUST define this: a custom engine owns the meaning
        of success for its protocol (siblings use ``error is None`` or
        a 2xx status). The body was ``raise True``, which raises
        ``TypeError: exceptions must derive from BaseException`` —
        and ``Results`` reads ``.successful`` for EVERY result, so a
        custom-engine run died in the reporting pass rather than at
        the point the contract was actually unmet.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement the 'successful' property"
        )