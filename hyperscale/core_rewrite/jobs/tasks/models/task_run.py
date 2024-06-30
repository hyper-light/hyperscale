import time
from typing import Any, Optional

import orjson
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)

from .run_status import RunStatus


class TaskRun(BaseModel):
    run_id: StrictInt
    status: RunStatus
    error: Optional[StrictStr] = None
    trace: Optional[StrictStr] = None
    start: StrictInt | StrictFloat = time.monotonic()
    end: Optional[StrictInt | StrictFloat] = None
    elapsed: StrictInt | StrictFloat = 0
    result: Optional[Any] = None

    def to_json(self):
        return orjson.dumps(
            {
                "run_id": self.run_id,
                "status": self.status.value,
                "error": self.error,
                "start": self.start,
                "end": self.end,
                "elapsed": self.elapsed,
            }
        )

    def to_data(self):
        return {
            "run_id": self.run_id,
            "status": self.status.value,
            "error": self.error,
            "start": self.start,
            "end": self.end,
            "elapsed": self.elapsed,
        }
