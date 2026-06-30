from typing import Any, Optional
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)

from .run_status import RunStatus
from .task_type import TaskType

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()


class TaskRun(BaseModel):
    run_id: StrictInt
    task_name: StrictStr
    status: RunStatus
    error: Optional[StrictStr] = None
    trace: Optional[StrictStr] = None
    start: StrictInt | StrictFloat = _DEFAULT_CLOCK.monotonic()
    end: Optional[StrictInt | StrictFloat] = None
    elapsed: StrictInt | StrictFloat = 0
    result: Optional[Any] = None
    task_type: TaskType = TaskType.CALLABLE

    def complete(self):
        return self.status in [RunStatus.COMPLETE, RunStatus.CANCELLED, RunStatus.FAILED]