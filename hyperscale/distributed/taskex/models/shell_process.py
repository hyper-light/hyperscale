from pydantic import (
    BaseModel,
    StrictInt,
    StrictFloat,
    StrictStr,
    StrictBytes,
)
from typing import Literal, Any, Dict, Tuple
from .run_status import RunStatus
from .task_type import TaskType

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()


CommandType = Literal['shell', 'subprocess']


class ShellProcess(BaseModel):
    run_id: StrictInt
    task_name: StrictStr
    process_id: StrictInt
    command: StrictStr
    status: RunStatus
    args: Tuple[str, ...] | None = None
    return_code: StrictInt | None = None
    env: Dict[str, Any] | None = None
    working_directory: StrictStr | None = None
    command_type: CommandType = 'subprocess'
    error: StrictStr | StrictBytes | None = None
    trace: StrictStr | StrictBytes | None = None
    start: StrictInt | StrictFloat = _DEFAULT_CLOCK.monotonic()
    end: StrictInt | StrictFloat | None = None
    elapsed: StrictInt | StrictFloat = 0
    result: StrictStr | StrictBytes | None = None
    task_type: TaskType = TaskType.SHELL

    def complete(self):
        return self.status in [RunStatus.COMPLETE, RunStatus.CANCELLED, RunStatus.FAILED]