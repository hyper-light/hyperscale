import os
from typing import Callable, Dict, Union

import psutil
from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr

PrimaryType = Union[str, int, float, bytes, bool]


class Env(BaseModel):
    MERCURY_SYNC_MONITOR_SAMPLE_WINDOW: StrictStr = "5s"
    MERCURY_SYNC_MONITOR_SAMPLE_INTERVAL: StrictStr | StrictInt | StrictFloat = 0.1
    MERCURY_SYNC_PROCESS_JOB_CPU_LIMIT: StrictFloat | StrictInt = 85
    MERCURY_SYNC_PROCESS_JOB_MEMORY_LIMIT: StrictInt | StrictFloat = 2048
    MERCURY_SYNC_CONNECT_RETRIES: StrictInt = 10
    MERCURY_SYNC_TCP_CONNECT_SECONDS: StrictStr = "10s"
    MERCURY_SYNC_CLEANUP_INTERVAL: StrictStr = "1m"
    MERCURY_SYNC_MAX_CONCURRENCY: StrictInt = 2048
    MERCURY_SYNC_AUTH_SECRET: StrictStr = "hyperscale"
    MERCURY_SYNC_LOGS_DIRECTORY: StrictStr = os.getcwd()
    MERCURY_SYNC_REQUEST_TIMEOUT: StrictStr = "30s"
    MERCURY_SYNC_LOG_LEVEL: StrictStr = "info"
    MERCURY_SYNC_TASK_RUNNER_MAX_THREADS: StrictInt = psutil.cpu_count(logical=False)
    MERCURY_SYNC_MAX_RUNNING_WORKFLOWS: StrictInt = 1
    MERCURY_SYNC_MAX_PENDING_WORKFLOWS: StrictInt = 100
    MERCURY_SYNC_CONTEXT_POLL_RATE: StrictStr = "0.1s"

    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            "MERCURY_SYNC_PROCESS_JOB_CPU_LIMIT": float,
            "MERCURY_SYNC_PROCESS_JOB_MEMORY_LIMIT": float,
            "MERCURY_SYNC_CONNECT_RETRIES": int,
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
