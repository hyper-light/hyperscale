from __future__ import annotations
import os
import orjson
from pydantic import BaseModel, StrictBool, StrictStr, StrictInt, StrictFloat
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
    MERCURY_SYNC_TLS_VERIFY_HOSTNAME: StrictStr = "false"  # Set to "true" in production
    
    # SWIM Protocol Settings
    SWIM_MAX_PROBE_TIMEOUT: StrictInt = 10
    SWIM_MIN_PROBE_TIMEOUT: StrictInt = 1
    SWIM_CURRENT_TIMEOUT: StrictInt = 2
    SWIM_UDP_POLL_INTERVAL: StrictInt = 2
    SWIM_SUSPICION_MIN_TIMEOUT: StrictFloat = 2.0
    SWIM_SUSPICION_MAX_TIMEOUT: StrictFloat = 15.0
    
    # Circuit Breaker Settings
    CIRCUIT_BREAKER_MAX_ERRORS: StrictInt = 3
    CIRCUIT_BREAKER_WINDOW_SECONDS: StrictFloat = 30.0
    CIRCUIT_BREAKER_HALF_OPEN_AFTER: StrictFloat = 10.0

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
            # SWIM settings
            "SWIM_MAX_PROBE_TIMEOUT": int,
            "SWIM_MIN_PROBE_TIMEOUT": int,
            "SWIM_CURRENT_TIMEOUT": int,
            "SWIM_UDP_POLL_INTERVAL": int,
            "SWIM_SUSPICION_MIN_TIMEOUT": float,
            "SWIM_SUSPICION_MAX_TIMEOUT": float,
            # Circuit breaker settings
            "CIRCUIT_BREAKER_MAX_ERRORS": int,
            "CIRCUIT_BREAKER_WINDOW_SECONDS": float,
            "CIRCUIT_BREAKER_HALF_OPEN_AFTER": float,
        }
    
    def get_swim_init_context(self) -> dict:
        """
        Get SWIM protocol init_context from environment settings.
        
        Note: The 'nodes' dict is created fresh each time as it needs
        to be unique per server instance (contains asyncio.Queue objects).
        """
        from collections import defaultdict
        import asyncio
        
        return {
            'max_probe_timeout': self.SWIM_MAX_PROBE_TIMEOUT,
            'min_probe_timeout': self.SWIM_MIN_PROBE_TIMEOUT,
            'current_timeout': self.SWIM_CURRENT_TIMEOUT,
            'nodes': defaultdict(asyncio.Queue),  # Required for probe cycle
            'udp_poll_interval': self.SWIM_UDP_POLL_INTERVAL,
            'suspicion_min_timeout': self.SWIM_SUSPICION_MIN_TIMEOUT,
            'suspicion_max_timeout': self.SWIM_SUSPICION_MAX_TIMEOUT,
        }
    
    def get_circuit_breaker_config(self) -> dict:
        """Get circuit breaker configuration from environment settings."""
        return {
            'max_errors': self.CIRCUIT_BREAKER_MAX_ERRORS,
            'window_seconds': self.CIRCUIT_BREAKER_WINDOW_SECONDS,
            'half_open_after': self.CIRCUIT_BREAKER_HALF_OPEN_AFTER,
        }
