import os
from typing import Callable, Dict, Union

from pydantic import (
    BaseModel,
    StrictInt,
    StrictStr,
)

PrimaryType = Union[str, int, float, bytes, bool]


class Env(BaseModel):
    MERCURY_SYNC_TCP_CONNECT_SECONDS: StrictStr = "10s"
    MERCURY_SYNC_CLEANUP_INTERVAL: StrictStr = "0.5s"
    MERCURY_SYNC_MAX_CONCURRENCY: StrictInt = 2048
    MERCURY_SYNC_AUTH_SECRET: StrictStr
    MERCURY_SYNC_LOGS_DIRECTORY: StrictStr = os.getcwd()
    MERCURY_SYNC_REQUEST_TIMEOUT: StrictStr = "30s"
    MERCURY_SYNC_LOG_LEVEL: StrictStr = "info"
    MERCURY_SYNC_TASK_RUNNER_MAX_THREADS: StrictInt = os.cpu_count()
    MERCURY_SYNC_MAX_RUNNING_WORKFLOWS: StrictInt = 1
    MERCURY_SYNC_MAX_PENDING_WORKFLOWS: StrictInt = 100
    MERCURY_SYNC_CONTEXT_POLL_RATE: StrictStr = "1s"

    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            "MERCURY_SYNC_TCP_CONNECT_SECONDS": str,
            "MERCURY_SYNC_CLEANUP_INTERVAL": str,
            "MERCURY_SYNC_MAX_CONCURRENCY": int,
            "MERCURY_SYNC_AUTH_SECRET": str,
            "MERCURY_SYNC_LOGS_DIRECTORY": str,
            "MERCURY_SYNC_REQUEST_TIMEOUT": str,
            "MERCURY_SYNC_LOG_LEVEL": str,
            "MERCURY_SYNC_TASK_RUNNER_MAX_THREADS": int,
            "MERCURY_SYNC_MAX_WORKFLOWS": int,
            "MERCURY_SYNC_CONTEXT_POLL_RATE": str,
        }
