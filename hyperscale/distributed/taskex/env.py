import os
from typing import Callable, Dict, Literal, Union

from pydantic import BaseModel, StrictInt, StrictStr

PrimaryType = Union[str, int, float, bytes, bool]


class Env(BaseModel):
    MERCURY_SYNC_EXECUTOR_TYPE: Literal["thread", "process", "none"] = "process"
    MERCURY_SYNC_LOG_LEVEL: StrictStr = "info"
    MERCURY_SYNC_CLEANUP_INTERVAL: StrictStr = "1s"
    MERCURY_SYNC_TASK_RUNNER_MAX_THREADS: StrictInt = os.cpu_count()
    MERCURY_SYNC_MAX_RUNNING_WORKFLOWS: StrictInt = 1
    MERCURY_SYNC_MAX_PENDING_WORKFLOWS: StrictInt = 100
    MERCURY_SYNC_CONTEXT_POLL_RATE: StrictStr = "0.1s"
    MERCURY_SYNC_SHUTDOWN_POLL_RATE: StrictStr = "0.1s"
    MERCURY_SYNC_DUPLICATE_JOB_POLICY: Literal["reject", "replace"] = "replace"

    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            "MERCURY_SYNC_EXECUTOR_TYPE": str,
            "MERCURY_SYNC_CLEANUP_INTERVAL": str,
            "MERCURY_SYNC_LOG_LEVEL": str,
            "MERCURY_SYNC_TASK_RUNNER_MAX_THREADS": int,
            "MERCURY_SYNC_MAX_WORKFLOWS": int,
            "MERCURY_SYNC_CONTEXT_POLL_RATE": str,
            "MERCURY_SYNC_SHUTDOWN_POLL_RATE": str,
            "MERCURY_SYNC_DUPLICATE_JOB_POLICY": str,
        }
