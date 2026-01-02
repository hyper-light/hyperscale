from __future__ import annotations
import os
import orjson
from pydantic import BaseModel, StrictBool, StrictStr, StrictInt
from typing import Callable, Dict, Literal, Union

PrimaryType = Union[str, int, float, bytes, bool]


class Env(BaseModel):
    MERCURY_SYNC_SERVER_URL: StrictStr | None = None
    MERCURY_SYNC_API_VERISON: StrictStr = "0.0.1"
    MERCURY_SYNC_TASK_EXECUTOR_TYPE: Literal["thread", "process", "none"] = "thread"
    MERCURY_SYNC_TCP_CONNECT_RETRIES: StrictInt = 3
    MERCURY_SYNC_UDP_CONNECT_RETRIES: StrictInt = 3
    MERCURY_SYNC_CLEANUP_INTERVAL: StrictStr = "0.25s"
    MERCURY_SYNC_MAX_CONCURRENCY: StrictInt = 4096
    MERCURY_SYNC_AUTH_SECRET: StrictStr = "hyperscale-dev-secret-change-in-prod"
    MERCURY_SYNC_AUTH_SECRET_PREVIOUS: StrictStr | None = None
    MERCURY_SYNC_LOGS_DIRECTORY: StrictStr = os.getcwd()
    MERCURY_SYNC_REQUEST_TIMEOUT: StrictStr = "30s"
    MERCURY_SYNC_LOG_LEVEL: StrictStr = "info"
    MERCURY_SYNC_TASK_RUNNER_MAX_THREADS: StrictInt = os.cpu_count()
    MERCURY_SYNC_MAX_REQUEST_CACHE_SIZE: StrictInt = 100
    MERCURY_SYNC_ENABLE_REQUEST_CACHING: StrictBool = False
    MERCURY_SYNC_VERIFY_SSL_CERT: Literal["REQUIRED", "OPTIONAL", "NONE"] = "REQUIRED"

    @classmethod
    def types_map(cls) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            "MERCURY_SYNC_SERVER_URL": str,
            "MERCURY_SYNC_API_VERISON": str,
            "MERCURY_SYNC_TASK_EXECUTOR_TYPE": str,
            "MERCURY_SYNC_TCP_CONNECT_RETRIES": int,
            "MERCURY_SYNC_UDP_CONNECT_RETRIES": int,
            "MERCURY_SYNC_CLEANUP_INTERVAL": str,
            "MERCURY_SYNC_MAX_CONCURRENCY": int,
            "MERCURY_SYNC_AUTH_SECRET": str,
            "MERCURY_SYNC_MULTICAST_GROUP": str,
            "MERCURY_SYNC_LOGS_DIRECTORY": str,
            "MERCURY_SYNC_REQUEST_TIMEOUT": str,
            "MERCURY_SYNC_LOG_LEVEL": str,
            "MERCURY_SYNC_TASK_RUNNER_MAX_THREADS": int,
            "MERCURY_SYNC_MAX_REQUEST_CACHE_SIZE": int,
            "MERCURY_SYNC_ENABLE_REQUEST_CACHING": str,
        }
