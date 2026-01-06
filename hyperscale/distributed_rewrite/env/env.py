from __future__ import annotations
import os
import orjson
from pydantic import BaseModel, StrictBool, StrictStr, StrictInt, StrictFloat
from typing import Callable, Dict, Literal, Union

PrimaryType = Union[str, int, float, bytes, bool]


class Env(BaseModel):
    MERCURY_SYNC_CONNECT_SECONDS: StrictStr = "5s"
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
    
    # Monitor Settings (for CPU/Memory monitors in workers)
    MERCURY_SYNC_MONITOR_SAMPLE_WINDOW: StrictStr = "5s"
    MERCURY_SYNC_MONITOR_SAMPLE_INTERVAL: StrictStr | StrictInt | StrictFloat = 0.1
    MERCURY_SYNC_PROCESS_JOB_CPU_LIMIT: StrictFloat | StrictInt = 85
    MERCURY_SYNC_PROCESS_JOB_MEMORY_LIMIT: StrictInt | StrictFloat = 2048
    
    # Local Server Pool / RemoteGraphManager Settings (used by workers)
    MERCURY_SYNC_CONNECT_TIMEOUT: StrictStr = "1s"
    MERCURY_SYNC_RETRY_INTERVAL: StrictStr = "1s"
    MERCURY_SYNC_SEND_RETRIES: StrictInt = 3
    MERCURY_SYNC_CONNECT_RETRIES: StrictInt = 10
    MERCURY_SYNC_MAX_RUNNING_WORKFLOWS: StrictInt = 1
    MERCURY_SYNC_MAX_PENDING_WORKFLOWS: StrictInt = 100
    MERCURY_SYNC_CONTEXT_POLL_RATE: StrictStr = "0.1s"
    MERCURY_SYNC_SHUTDOWN_POLL_RATE: StrictStr = "0.1s"
    MERCURY_SYNC_DUPLICATE_JOB_POLICY: Literal["reject", "replace"] = "replace"
    
    # SWIM Protocol Settings
    SWIM_MAX_PROBE_TIMEOUT: StrictInt = 10
    SWIM_MIN_PROBE_TIMEOUT: StrictInt = 1
    SWIM_CURRENT_TIMEOUT: StrictInt = 2
    SWIM_UDP_POLL_INTERVAL: StrictInt = 2
    SWIM_SUSPICION_MIN_TIMEOUT: StrictFloat = 2.0
    SWIM_SUSPICION_MAX_TIMEOUT: StrictFloat = 15.0
    
    # Leader Election Settings
    LEADER_HEARTBEAT_INTERVAL: StrictFloat = 2.0  # Seconds between leader heartbeats
    LEADER_ELECTION_TIMEOUT_BASE: StrictFloat = 5.0  # Base election timeout
    LEADER_ELECTION_TIMEOUT_JITTER: StrictFloat = 2.0  # Random jitter added to timeout
    LEADER_PRE_VOTE_TIMEOUT: StrictFloat = 2.0  # Timeout for pre-vote phase
    LEADER_LEASE_DURATION: StrictFloat = 5.0  # Leader lease duration in seconds
    LEADER_MAX_LHM: StrictInt = 4  # Max LHM score for leader eligibility (higher = more tolerant)

    # Cluster Formation Settings
    CLUSTER_STABILIZATION_TIMEOUT: StrictFloat = 10.0  # Max seconds to wait for cluster to form
    CLUSTER_STABILIZATION_POLL_INTERVAL: StrictFloat = 0.5  # How often to check cluster membership
    LEADER_ELECTION_JITTER_MAX: StrictFloat = 3.0  # Max random delay before starting first election
    
    # Federated Health Monitor Settings (Gate -> DC Leader probing)
    # These are tuned for high-latency, globally distributed links
    FEDERATED_PROBE_INTERVAL: StrictFloat = 2.0  # Seconds between probes to each DC
    FEDERATED_PROBE_TIMEOUT: StrictFloat = 5.0  # Timeout for single probe (high for cross-DC)
    FEDERATED_SUSPICION_TIMEOUT: StrictFloat = 30.0  # Time before suspected -> unreachable
    FEDERATED_MAX_CONSECUTIVE_FAILURES: StrictInt = 5  # Failures before marking suspected
    
    # Circuit Breaker Settings
    CIRCUIT_BREAKER_MAX_ERRORS: StrictInt = 3
    CIRCUIT_BREAKER_WINDOW_SECONDS: StrictFloat = 30.0
    CIRCUIT_BREAKER_HALF_OPEN_AFTER: StrictFloat = 10.0

    # Worker Progress Update Settings
    WORKER_PROGRESS_UPDATE_INTERVAL: StrictFloat = 1.0  # How often to collect progress locally
    WORKER_PROGRESS_FLUSH_INTERVAL: StrictFloat = 2.0  # How often to send buffered updates to manager
    WORKER_MAX_CORES: StrictInt | None = None

    # Worker Dead Manager Cleanup Settings
    WORKER_DEAD_MANAGER_REAP_INTERVAL: StrictFloat = 900.0  # Seconds before reaping dead managers (15 minutes)

    # Worker Cancellation Polling Settings
    WORKER_CANCELLATION_POLL_INTERVAL: StrictFloat = 5.0  # Seconds between cancellation poll requests

    # Manager Startup and Dispatch Settings
    MANAGER_STARTUP_SYNC_DELAY: StrictFloat = 2.0  # Seconds to wait for leader election before state sync
    MANAGER_STATE_SYNC_TIMEOUT: StrictFloat = 5.0  # Timeout for state sync request to leader
    MANAGER_STATE_SYNC_RETRIES: StrictInt = 3  # Number of retries for state sync
    MANAGER_DISPATCH_CORE_WAIT_TIMEOUT: StrictFloat = 5.0  # Max seconds to wait per iteration for cores
    MANAGER_HEARTBEAT_INTERVAL: StrictFloat = 5.0  # Seconds between manager heartbeats to gates
    MANAGER_PEER_SYNC_INTERVAL: StrictFloat = 10.0  # Seconds between job state sync to peer managers

    # Job Cleanup Settings
    COMPLETED_JOB_MAX_AGE: StrictFloat = 300.0  # Seconds to retain completed jobs (5 minutes)
    FAILED_JOB_MAX_AGE: StrictFloat = 3600.0  # Seconds to retain failed/cancelled/timeout jobs (1 hour)
    JOB_CLEANUP_INTERVAL: StrictFloat = 60.0  # Seconds between cleanup checks

    # Manager Dead Node Cleanup Settings
    MANAGER_DEAD_WORKER_REAP_INTERVAL: StrictFloat = 900.0  # Seconds before reaping dead workers (15 minutes)
    MANAGER_DEAD_PEER_REAP_INTERVAL: StrictFloat = 900.0  # Seconds before reaping dead manager peers (15 minutes)
    MANAGER_DEAD_GATE_REAP_INTERVAL: StrictFloat = 900.0  # Seconds before reaping dead gates (15 minutes)

    @classmethod
    def types_map(cls) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            "MERCURY_SYNC_CONNECT_SECONDS": str,
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
            # Monitor settings
            "MERCURY_SYNC_MONITOR_SAMPLE_WINDOW": str,
            "MERCURY_SYNC_MONITOR_SAMPLE_INTERVAL": float,
            "MERCURY_SYNC_PROCESS_JOB_CPU_LIMIT": float,
            "MERCURY_SYNC_PROCESS_JOB_MEMORY_LIMIT": float,
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
            # Leader election settings
            "LEADER_HEARTBEAT_INTERVAL": float,
            "LEADER_ELECTION_TIMEOUT_BASE": float,
            "LEADER_ELECTION_TIMEOUT_JITTER": float,
            "LEADER_PRE_VOTE_TIMEOUT": float,
            "LEADER_LEASE_DURATION": float,
            "LEADER_MAX_LHM": int,
            # Cluster formation settings
            "CLUSTER_STABILIZATION_TIMEOUT": float,
            "CLUSTER_STABILIZATION_POLL_INTERVAL": float,
            "LEADER_ELECTION_JITTER_MAX": float,
            # Federated health monitor settings
            "FEDERATED_PROBE_INTERVAL": float,
            "FEDERATED_PROBE_TIMEOUT": float,
            "FEDERATED_SUSPICION_TIMEOUT": float,
            "FEDERATED_MAX_CONSECUTIVE_FAILURES": int,
            # Worker progress update settings
            "WORKER_PROGRESS_UPDATE_INTERVAL": float,
            "WORKER_PROGRESS_FLUSH_INTERVAL": float,
            "WORKER_MAX_CORES": int,
            # Worker dead manager cleanup settings
            "WORKER_DEAD_MANAGER_REAP_INTERVAL": float,
            # Worker cancellation polling settings
            "WORKER_CANCELLATION_POLL_INTERVAL": float,
            # Manager startup and dispatch settings
            "MANAGER_STARTUP_SYNC_DELAY": float,
            "MANAGER_STATE_SYNC_TIMEOUT": float,
            "MANAGER_STATE_SYNC_RETRIES": int,
            "MANAGER_DISPATCH_CORE_WAIT_TIMEOUT": float,
            "MANAGER_HEARTBEAT_INTERVAL": float,
            "MANAGER_PEER_SYNC_INTERVAL": float,
            # Job cleanup settings
            "COMPLETED_JOB_MAX_AGE": float,
            "FAILED_JOB_MAX_AGE": float,
            "JOB_CLEANUP_INTERVAL": float,
            # Manager dead node cleanup settings
            "MANAGER_DEAD_WORKER_REAP_INTERVAL": float,
            "MANAGER_DEAD_PEER_REAP_INTERVAL": float,
            "MANAGER_DEAD_GATE_REAP_INTERVAL": float,
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
    
    def get_leader_election_config(self) -> dict:
        """
        Get leader election configuration from environment settings.
        
        These settings control:
        - How often the leader sends heartbeats
        - How long followers wait before starting an election
        - Leader lease duration for failure detection
        - LHM threshold for leader eligibility (higher = more tolerant to load)
        """
        return {
            'heartbeat_interval': self.LEADER_HEARTBEAT_INTERVAL,
            'election_timeout_base': self.LEADER_ELECTION_TIMEOUT_BASE,
            'election_timeout_jitter': self.LEADER_ELECTION_TIMEOUT_JITTER,
            'pre_vote_timeout': self.LEADER_PRE_VOTE_TIMEOUT,
            'lease_duration': self.LEADER_LEASE_DURATION,
            'max_leader_lhm': self.LEADER_MAX_LHM,
        }
    
    def get_federated_health_config(self) -> dict:
        """
        Get federated health monitor configuration from environment settings.
        
        These settings are tuned for high-latency, globally distributed links
        between gates and datacenter managers:
        - Longer probe intervals (reduce cross-DC traffic)
        - Longer timeouts (accommodate high latency)
        - Longer suspicion period (tolerate transient issues)
        """
        return {
            'probe_interval': self.FEDERATED_PROBE_INTERVAL,
            'probe_timeout': self.FEDERATED_PROBE_TIMEOUT,
            'suspicion_timeout': self.FEDERATED_SUSPICION_TIMEOUT,
            'max_consecutive_failures': self.FEDERATED_MAX_CONSECUTIVE_FAILURES,
        }
