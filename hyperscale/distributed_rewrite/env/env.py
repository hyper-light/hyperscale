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
    # Tuned for faster failure detection while avoiding false positives:
    # - Total detection time: ~4-8 seconds (probe timeout + suspicion)
    # - Previous: ~6-15 seconds
    SWIM_MAX_PROBE_TIMEOUT: StrictInt = 5  # Reduced from 10 - faster failure escalation
    SWIM_MIN_PROBE_TIMEOUT: StrictInt = 1
    SWIM_CURRENT_TIMEOUT: StrictInt = 1  # Reduced from 2 - faster initial probe timeout
    SWIM_UDP_POLL_INTERVAL: StrictInt = 1  # Reduced from 2 - more frequent probing
    SWIM_SUSPICION_MIN_TIMEOUT: StrictFloat = 1.5  # Reduced from 2.0 - faster confirmation
    SWIM_SUSPICION_MAX_TIMEOUT: StrictFloat = 8.0  # Reduced from 15.0 - faster failure declaration
    # Refutation rate limiting - prevents incarnation exhaustion attacks
    # If an attacker sends many probes/suspects about us, we limit how fast we increment incarnation
    SWIM_REFUTATION_RATE_LIMIT_TOKENS: StrictInt = 5  # Max refutations per window
    SWIM_REFUTATION_RATE_LIMIT_WINDOW: StrictFloat = 10.0  # Window duration in seconds
    
    # Leader Election Settings
    LEADER_HEARTBEAT_INTERVAL: StrictFloat = 2.0  # Seconds between leader heartbeats
    LEADER_ELECTION_TIMEOUT_BASE: StrictFloat = 5.0  # Base election timeout
    LEADER_ELECTION_TIMEOUT_JITTER: StrictFloat = 2.0  # Random jitter added to timeout
    LEADER_PRE_VOTE_TIMEOUT: StrictFloat = 2.0  # Timeout for pre-vote phase
    LEADER_LEASE_DURATION: StrictFloat = 5.0  # Leader lease duration in seconds
    LEADER_MAX_LHM: StrictInt = 4  # Max LHM score for leader eligibility (higher = more tolerant)

    # Job Lease Settings (Gate per-job ownership)
    JOB_LEASE_DURATION: StrictFloat = 30.0  # Duration of job ownership lease in seconds
    JOB_LEASE_CLEANUP_INTERVAL: StrictFloat = 10.0  # How often to clean up expired job leases

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

    # Worker Progress Update Settings (tuned for real-time terminal UI)
    WORKER_PROGRESS_UPDATE_INTERVAL: StrictFloat = 0.05  # How often to collect progress locally (50ms)
    WORKER_PROGRESS_FLUSH_INTERVAL: StrictFloat = 0.05  # How often to send buffered updates to manager (50ms)
    WORKER_MAX_CORES: StrictInt | None = None

    # Worker Dead Manager Cleanup Settings
    WORKER_DEAD_MANAGER_REAP_INTERVAL: StrictFloat = 900.0  # Seconds before reaping dead managers (15 minutes)
    WORKER_DEAD_MANAGER_CHECK_INTERVAL: StrictFloat = 60.0  # Seconds between dead manager checks

    # Worker Cancellation Polling Settings
    WORKER_CANCELLATION_POLL_INTERVAL: StrictFloat = 5.0  # Seconds between cancellation poll requests

    # Worker TCP Timeout Settings
    WORKER_TCP_TIMEOUT_SHORT: StrictFloat = 2.0  # Short timeout for quick operations
    WORKER_TCP_TIMEOUT_STANDARD: StrictFloat = 5.0  # Standard timeout for progress/result pushes

    # Worker Orphan Grace Period Settings (Section 2.7)
    # Grace period before cancelling workflows when job leader manager fails
    # Should be longer than expected election + takeover time
    WORKER_ORPHAN_GRACE_PERIOD: StrictFloat = 5.0  # Seconds to wait for JobLeaderWorkerTransfer
    WORKER_ORPHAN_CHECK_INTERVAL: StrictFloat = 1.0  # Seconds between orphan grace period checks

    # Worker Job Leadership Transfer Settings (Section 8)
    # TTL for pending transfers that arrive before workflows are known
    WORKER_PENDING_TRANSFER_TTL: StrictFloat = 60.0  # Seconds to retain pending transfers

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

    # Cancelled Workflow Cleanup Settings (Section 6)
    CANCELLED_WORKFLOW_TTL: StrictFloat = 3600.0  # Seconds to retain cancelled workflow info (1 hour)
    CANCELLED_WORKFLOW_CLEANUP_INTERVAL: StrictFloat = 60.0  # Seconds between cleanup checks

    # Client Leadership Transfer Settings (Section 9)
    CLIENT_ORPHAN_GRACE_PERIOD: StrictFloat = 15.0  # Seconds to wait for leadership transfer cascade
    CLIENT_ORPHAN_CHECK_INTERVAL: StrictFloat = 2.0  # Seconds between orphan grace period checks
    CLIENT_RESPONSE_FRESHNESS_TIMEOUT: StrictFloat = 10.0  # Seconds to consider response stale after leadership change

    # Manager Dead Node Cleanup Settings
    MANAGER_DEAD_WORKER_REAP_INTERVAL: StrictFloat = 900.0  # Seconds before reaping dead workers (15 minutes)
    MANAGER_DEAD_PEER_REAP_INTERVAL: StrictFloat = 900.0  # Seconds before reaping dead manager peers (15 minutes)
    MANAGER_DEAD_GATE_REAP_INTERVAL: StrictFloat = 900.0  # Seconds before reaping dead gates (15 minutes)
    MANAGER_DEAD_NODE_CHECK_INTERVAL: StrictFloat = 60.0  # Seconds between dead node checks
    MANAGER_RATE_LIMIT_CLEANUP_INTERVAL: StrictFloat = 60.0  # Seconds between rate limit client cleanup

    # Manager TCP Timeout Settings
    MANAGER_TCP_TIMEOUT_SHORT: StrictFloat = 2.0  # Short timeout for quick operations (peer sync, worker queries)
    MANAGER_TCP_TIMEOUT_STANDARD: StrictFloat = 5.0  # Standard timeout for job dispatch, result forwarding

    # Manager Batch Stats Settings
    MANAGER_BATCH_PUSH_INTERVAL: StrictFloat = 0.25  # Seconds between batch stats pushes to clients (when no gates)

    # ==========================================================================
    # Gate Settings
    # ==========================================================================
    GATE_JOB_CLEANUP_INTERVAL: StrictFloat = 60.0  # Seconds between job cleanup checks
    GATE_RATE_LIMIT_CLEANUP_INTERVAL: StrictFloat = 60.0  # Seconds between rate limit client cleanup
    GATE_BATCH_STATS_INTERVAL: StrictFloat = 0.25  # Seconds between batch stats pushes to clients
    GATE_TCP_TIMEOUT_SHORT: StrictFloat = 2.0  # Short timeout for quick operations
    GATE_TCP_TIMEOUT_STANDARD: StrictFloat = 5.0  # Standard timeout for job dispatch, result forwarding
    GATE_TCP_TIMEOUT_FORWARD: StrictFloat = 3.0  # Timeout for forwarding to peers

    # Gate Orphan Job Grace Period Settings (Section 7)
    # Grace period before marking orphaned jobs as failed when job leader manager dies
    # Should be longer than expected election + takeover time
    GATE_ORPHAN_GRACE_PERIOD: StrictFloat = 10.0  # Seconds to wait for JobLeaderGateTransfer
    GATE_ORPHAN_CHECK_INTERVAL: StrictFloat = 2.0  # Seconds between orphan grace period checks

    # ==========================================================================
    # Overload Detection Settings (AD-18)
    # ==========================================================================
    OVERLOAD_EMA_ALPHA: StrictFloat = 0.1  # Smoothing factor for baseline (lower = more stable)
    OVERLOAD_CURRENT_WINDOW: StrictInt = 10  # Samples for current average
    OVERLOAD_TREND_WINDOW: StrictInt = 20  # Samples for trend calculation
    OVERLOAD_MIN_SAMPLES: StrictInt = 3  # Minimum samples before delta detection
    OVERLOAD_TREND_THRESHOLD: StrictFloat = 0.1  # Rising trend threshold
    # Delta thresholds (% above baseline): busy / stressed / overloaded
    OVERLOAD_DELTA_BUSY: StrictFloat = 0.2  # 20% above baseline
    OVERLOAD_DELTA_STRESSED: StrictFloat = 0.5  # 50% above baseline
    OVERLOAD_DELTA_OVERLOADED: StrictFloat = 1.0  # 100% above baseline
    # Absolute bounds (milliseconds): busy / stressed / overloaded
    OVERLOAD_ABSOLUTE_BUSY_MS: StrictFloat = 200.0
    OVERLOAD_ABSOLUTE_STRESSED_MS: StrictFloat = 500.0
    OVERLOAD_ABSOLUTE_OVERLOADED_MS: StrictFloat = 2000.0
    # CPU thresholds (0.0 to 1.0): busy / stressed / overloaded
    OVERLOAD_CPU_BUSY: StrictFloat = 0.7
    OVERLOAD_CPU_STRESSED: StrictFloat = 0.85
    OVERLOAD_CPU_OVERLOADED: StrictFloat = 0.95
    # Memory thresholds (0.0 to 1.0): busy / stressed / overloaded
    OVERLOAD_MEMORY_BUSY: StrictFloat = 0.7
    OVERLOAD_MEMORY_STRESSED: StrictFloat = 0.85
    OVERLOAD_MEMORY_OVERLOADED: StrictFloat = 0.95

    # ==========================================================================
    # Health Probe Settings (AD-19)
    # ==========================================================================
    # Liveness probe settings
    LIVENESS_PROBE_TIMEOUT: StrictFloat = 1.0  # Seconds
    LIVENESS_PROBE_PERIOD: StrictFloat = 10.0  # Seconds between checks
    LIVENESS_PROBE_FAILURE_THRESHOLD: StrictInt = 3  # Failures before unhealthy
    LIVENESS_PROBE_SUCCESS_THRESHOLD: StrictInt = 1  # Successes to recover
    # Readiness probe settings
    READINESS_PROBE_TIMEOUT: StrictFloat = 2.0  # Seconds
    READINESS_PROBE_PERIOD: StrictFloat = 10.0  # Seconds between checks
    READINESS_PROBE_FAILURE_THRESHOLD: StrictInt = 3  # Failures before unhealthy
    READINESS_PROBE_SUCCESS_THRESHOLD: StrictInt = 1  # Successes to recover
    # Startup probe settings
    STARTUP_PROBE_TIMEOUT: StrictFloat = 5.0  # Seconds
    STARTUP_PROBE_PERIOD: StrictFloat = 5.0  # Seconds between checks
    STARTUP_PROBE_FAILURE_THRESHOLD: StrictInt = 30  # Allow slow startups (150s)
    STARTUP_PROBE_SUCCESS_THRESHOLD: StrictInt = 1  # One success = started

    # ==========================================================================
    # Rate Limiting Settings (AD-24)
    # ==========================================================================
    RATE_LIMIT_DEFAULT_BUCKET_SIZE: StrictInt = 100  # Default token bucket size
    RATE_LIMIT_DEFAULT_REFILL_RATE: StrictFloat = 10.0  # Tokens per second
    RATE_LIMIT_CLIENT_IDLE_TIMEOUT: StrictFloat = 300.0  # Cleanup idle clients after 5min
    RATE_LIMIT_CLEANUP_INTERVAL: StrictFloat = 60.0  # Run cleanup every minute
    RATE_LIMIT_MAX_RETRIES: StrictInt = 3  # Max retry attempts when rate limited
    RATE_LIMIT_MAX_TOTAL_WAIT: StrictFloat = 60.0  # Max total wait time for retries
    RATE_LIMIT_BACKOFF_MULTIPLIER: StrictFloat = 1.5  # Backoff multiplier for retries

    # ==========================================================================
    # Recovery and Thundering Herd Prevention Settings
    # ==========================================================================
    # Jitter settings - applied to recovery operations to prevent synchronized reconnection waves
    # Reduced from 0.1-2.0s to 0.05-0.5s for faster recovery while still preventing thundering herd
    RECOVERY_JITTER_MAX: StrictFloat = 0.5  # Reduced from 2.0 - faster recovery
    RECOVERY_JITTER_MIN: StrictFloat = 0.05  # Reduced from 0.1 - minimal delay

    # Concurrency caps - limit simultaneous recovery operations to prevent overload
    RECOVERY_MAX_CONCURRENT: StrictInt = 5  # Max concurrent recovery operations per node type
    RECOVERY_SEMAPHORE_SIZE: StrictInt = 5  # Semaphore size for limiting concurrent recovery
    DISPATCH_MAX_CONCURRENT_PER_WORKER: StrictInt = 3  # Max concurrent dispatches to a single worker

    # Message queue backpressure - prevent memory exhaustion under load
    MESSAGE_QUEUE_MAX_SIZE: StrictInt = 1000  # Max pending messages per client connection
    MESSAGE_QUEUE_WARN_SIZE: StrictInt = 800  # Warn threshold (80% of max)

    # ==========================================================================
    # Healthcheck Extension Settings (AD-26)
    # ==========================================================================
    EXTENSION_BASE_DEADLINE: StrictFloat = 30.0  # Base deadline in seconds
    EXTENSION_MIN_GRANT: StrictFloat = 1.0  # Minimum extension grant in seconds
    EXTENSION_MAX_EXTENSIONS: StrictInt = 5  # Maximum extensions per cycle
    EXTENSION_EVICTION_THRESHOLD: StrictInt = 3  # Failures before eviction
    EXTENSION_EXHAUSTION_WARNING_THRESHOLD: StrictInt = 1  # Remaining extensions to trigger warning
    EXTENSION_EXHAUSTION_GRACE_PERIOD: StrictFloat = 10.0  # Seconds of grace after exhaustion before kill

    # ==========================================================================
    # Orphaned Workflow Scanner Settings
    # ==========================================================================
    ORPHAN_SCAN_INTERVAL: StrictFloat = 120.0  # Seconds between orphan scans (2 minutes)
    ORPHAN_SCAN_WORKER_TIMEOUT: StrictFloat = 5.0  # Timeout for querying workers during scan

    # ==========================================================================
    # Time-Windowed Stats Streaming Settings
    # ==========================================================================
    STATS_WINDOW_SIZE_MS: StrictFloat = 50.0  # Window bucket size in milliseconds (smaller = more granular)
    # Drift tolerance allows for network latency between worker send and manager receive
    # Workers now send directly (not buffered), so we only need network latency margin
    STATS_DRIFT_TOLERANCE_MS: StrictFloat = 25.0  # Network latency allowance only
    STATS_PUSH_INTERVAL_MS: StrictFloat = 50.0  # How often to flush windows and push (ms)
    STATS_MAX_WINDOW_AGE_MS: StrictFloat = 5000.0  # Max age before window is dropped (cleanup)

    # Status update processing interval (seconds) - controls how often _process_status_updates runs
    # during workflow completion wait. Lower values = more responsive UI updates.
    STATUS_UPDATE_POLL_INTERVAL: StrictFloat = 0.05  # 50ms default for real-time UI

    # Client rate limiting for progress updates only
    CLIENT_PROGRESS_RATE_LIMIT: StrictFloat = 100.0  # Max progress callbacks per second
    CLIENT_PROGRESS_BURST: StrictInt = 20  # Burst allowance for progress callbacks

    # ==========================================================================
    # Cross-DC Correlation Settings (Phase 7)
    # ==========================================================================
    # These settings control correlation detection for cascade eviction prevention
    # Tuned for globally distributed datacenters with high latency
    CROSS_DC_CORRELATION_WINDOW: StrictFloat = 30.0  # Seconds window for correlation detection
    CROSS_DC_CORRELATION_LOW_THRESHOLD: StrictInt = 2  # Min DCs failing for LOW correlation
    CROSS_DC_CORRELATION_MEDIUM_THRESHOLD: StrictInt = 3  # Min DCs failing for MEDIUM correlation
    CROSS_DC_CORRELATION_HIGH_COUNT_THRESHOLD: StrictInt = 4  # Min DCs failing for HIGH (count)
    CROSS_DC_CORRELATION_HIGH_FRACTION: StrictFloat = 0.5  # Fraction of DCs for HIGH (requires count too)
    CROSS_DC_CORRELATION_BACKOFF: StrictFloat = 60.0  # Backoff duration after correlation detected

    # Anti-flapping settings for cross-DC correlation
    CROSS_DC_FAILURE_CONFIRMATION: StrictFloat = 5.0  # Seconds failure must persist before counting
    CROSS_DC_RECOVERY_CONFIRMATION: StrictFloat = 30.0  # Seconds recovery must persist before healthy
    CROSS_DC_FLAP_THRESHOLD: StrictInt = 3  # State changes in window to be considered flapping
    CROSS_DC_FLAP_DETECTION_WINDOW: StrictFloat = 120.0  # Window for flap detection
    CROSS_DC_FLAP_COOLDOWN: StrictFloat = 300.0  # Cooldown after flapping before can be stable

    # Latency-based correlation settings
    CROSS_DC_ENABLE_LATENCY_CORRELATION: StrictBool = True
    CROSS_DC_LATENCY_ELEVATED_THRESHOLD_MS: StrictFloat = 100.0  # Latency above this is elevated
    CROSS_DC_LATENCY_CRITICAL_THRESHOLD_MS: StrictFloat = 500.0  # Latency above this is critical
    CROSS_DC_MIN_LATENCY_SAMPLES: StrictInt = 3  # Min samples before latency decisions
    CROSS_DC_LATENCY_SAMPLE_WINDOW: StrictFloat = 60.0  # Window for latency samples
    CROSS_DC_LATENCY_CORRELATION_FRACTION: StrictFloat = 0.5  # Fraction of DCs for latency correlation

    # Extension-based correlation settings
    CROSS_DC_ENABLE_EXTENSION_CORRELATION: StrictBool = True
    CROSS_DC_EXTENSION_COUNT_THRESHOLD: StrictInt = 2  # Extensions to consider DC under load
    CROSS_DC_EXTENSION_CORRELATION_FRACTION: StrictFloat = 0.5  # Fraction of DCs for extension correlation
    CROSS_DC_EXTENSION_WINDOW: StrictFloat = 120.0  # Window for extension tracking

    # LHM-based correlation settings
    CROSS_DC_ENABLE_LHM_CORRELATION: StrictBool = True
    CROSS_DC_LHM_STRESSED_THRESHOLD: StrictInt = 3  # LHM score (0-8) to consider DC stressed
    CROSS_DC_LHM_CORRELATION_FRACTION: StrictFloat = 0.5  # Fraction of DCs for LHM correlation

    # ==========================================================================
    # Discovery Service Settings (AD-28)
    # ==========================================================================
    # DNS-based peer discovery
    DISCOVERY_DNS_NAMES: StrictStr = ""  # Comma-separated DNS names for manager discovery
    DISCOVERY_DNS_CACHE_TTL: StrictFloat = 60.0  # DNS cache TTL in seconds
    DISCOVERY_DNS_TIMEOUT: StrictFloat = 5.0  # DNS resolution timeout in seconds
    DISCOVERY_DEFAULT_PORT: StrictInt = 9091  # Default port for discovered peers

    # DNS Security (Phase 2) - Protects against cache poisoning, hijacking, spoofing
    DISCOVERY_DNS_ALLOWED_CIDRS: StrictStr = ""  # Comma-separated CIDRs (e.g., "10.0.0.0/8,172.16.0.0/12")
    DISCOVERY_DNS_BLOCK_PRIVATE_FOR_PUBLIC: StrictBool = False  # Block private IPs for public hostnames
    DISCOVERY_DNS_DETECT_IP_CHANGES: StrictBool = True  # Enable IP change anomaly detection
    DISCOVERY_DNS_MAX_IP_CHANGES: StrictInt = 5  # Max IP changes before rapid rotation alert
    DISCOVERY_DNS_IP_CHANGE_WINDOW: StrictFloat = 300.0  # Window for tracking IP changes (5 min)
    DISCOVERY_DNS_REJECT_ON_VIOLATION: StrictBool = True  # Reject IPs failing security validation

    # Locality configuration
    DISCOVERY_DATACENTER_ID: StrictStr = ""  # Local datacenter ID for locality-aware selection
    DISCOVERY_REGION_ID: StrictStr = ""  # Local region ID for locality-aware selection
    DISCOVERY_PREFER_SAME_DC: StrictBool = True  # Prefer same-DC peers over cross-DC

    # Adaptive peer selection (Power of Two Choices with EWMA)
    DISCOVERY_CANDIDATE_SET_SIZE: StrictInt = 3  # Number of candidates for power-of-two selection
    DISCOVERY_EWMA_ALPHA: StrictFloat = 0.3  # EWMA smoothing factor for latency tracking
    DISCOVERY_BASELINE_LATENCY_MS: StrictFloat = 50.0  # Baseline latency for EWMA initialization
    DISCOVERY_LATENCY_MULTIPLIER_THRESHOLD: StrictFloat = 2.0  # Latency threshold multiplier
    DISCOVERY_MIN_PEERS_PER_TIER: StrictInt = 1  # Minimum peers per locality tier

    # Probing and health
    DISCOVERY_MAX_CONCURRENT_PROBES: StrictInt = 10  # Max concurrent DNS resolutions/probes
    DISCOVERY_PROBE_INTERVAL: StrictFloat = 30.0  # Seconds between peer health probes
    DISCOVERY_FAILURE_DECAY_INTERVAL: StrictFloat = 60.0  # Seconds between failure count decay

    # ==========================================================================
    # Bounded Pending Response Queues Settings (AD-32)
    # ==========================================================================
    # Priority-aware bounded execution with load shedding
    # CRITICAL (SWIM) never shed, LOW shed first under load
    PENDING_RESPONSE_MAX_CONCURRENT: StrictInt = 1000  # Global limit across all priorities
    PENDING_RESPONSE_HIGH_LIMIT: StrictInt = 500  # HIGH priority limit
    PENDING_RESPONSE_NORMAL_LIMIT: StrictInt = 300  # NORMAL priority limit
    PENDING_RESPONSE_LOW_LIMIT: StrictInt = 200  # LOW priority limit (shed first)
    PENDING_RESPONSE_WARN_THRESHOLD: StrictFloat = 0.8  # Log warning at this % of global limit

    # Client-side per-destination queue settings (AD-32)
    OUTGOING_QUEUE_SIZE: StrictInt = 500  # Per-destination queue size
    OUTGOING_OVERFLOW_SIZE: StrictInt = 100  # Overflow ring buffer size
    OUTGOING_MAX_DESTINATIONS: StrictInt = 1000  # Max tracked destinations (LRU evicted)

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
            "SWIM_REFUTATION_RATE_LIMIT_TOKENS": int,
            "SWIM_REFUTATION_RATE_LIMIT_WINDOW": float,
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
            "WORKER_DEAD_MANAGER_CHECK_INTERVAL": float,
            # Worker cancellation polling settings
            "WORKER_CANCELLATION_POLL_INTERVAL": float,
            # Worker TCP timeout settings
            "WORKER_TCP_TIMEOUT_SHORT": float,
            "WORKER_TCP_TIMEOUT_STANDARD": float,
            # Worker orphan grace period settings
            "WORKER_ORPHAN_GRACE_PERIOD": float,
            "WORKER_ORPHAN_CHECK_INTERVAL": float,
            # Worker job leadership transfer settings (Section 8)
            "WORKER_PENDING_TRANSFER_TTL": float,
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
            # Cancelled workflow cleanup settings (Section 6)
            "CANCELLED_WORKFLOW_TTL": float,
            "CANCELLED_WORKFLOW_CLEANUP_INTERVAL": float,
            # Client leadership transfer settings (Section 9)
            "CLIENT_ORPHAN_GRACE_PERIOD": float,
            "CLIENT_ORPHAN_CHECK_INTERVAL": float,
            "CLIENT_RESPONSE_FRESHNESS_TIMEOUT": float,
            # Manager dead node cleanup settings
            "MANAGER_DEAD_WORKER_REAP_INTERVAL": float,
            "MANAGER_DEAD_PEER_REAP_INTERVAL": float,
            "MANAGER_DEAD_GATE_REAP_INTERVAL": float,
            "MANAGER_DEAD_NODE_CHECK_INTERVAL": float,
            "MANAGER_RATE_LIMIT_CLEANUP_INTERVAL": float,
            # Manager TCP timeout settings
            "MANAGER_TCP_TIMEOUT_SHORT": float,
            "MANAGER_TCP_TIMEOUT_STANDARD": float,
            # Manager batch stats settings
            "MANAGER_BATCH_PUSH_INTERVAL": float,
            # Gate settings
            "GATE_JOB_CLEANUP_INTERVAL": float,
            "GATE_RATE_LIMIT_CLEANUP_INTERVAL": float,
            "GATE_BATCH_STATS_INTERVAL": float,
            "GATE_TCP_TIMEOUT_SHORT": float,
            "GATE_TCP_TIMEOUT_STANDARD": float,
            "GATE_TCP_TIMEOUT_FORWARD": float,
            # Gate orphan grace period settings (Section 7)
            "GATE_ORPHAN_GRACE_PERIOD": float,
            "GATE_ORPHAN_CHECK_INTERVAL": float,
            # Overload detection settings (AD-18)
            "OVERLOAD_EMA_ALPHA": float,
            "OVERLOAD_CURRENT_WINDOW": int,
            "OVERLOAD_TREND_WINDOW": int,
            "OVERLOAD_MIN_SAMPLES": int,
            "OVERLOAD_TREND_THRESHOLD": float,
            "OVERLOAD_DELTA_BUSY": float,
            "OVERLOAD_DELTA_STRESSED": float,
            "OVERLOAD_DELTA_OVERLOADED": float,
            "OVERLOAD_ABSOLUTE_BUSY_MS": float,
            "OVERLOAD_ABSOLUTE_STRESSED_MS": float,
            "OVERLOAD_ABSOLUTE_OVERLOADED_MS": float,
            "OVERLOAD_CPU_BUSY": float,
            "OVERLOAD_CPU_STRESSED": float,
            "OVERLOAD_CPU_OVERLOADED": float,
            "OVERLOAD_MEMORY_BUSY": float,
            "OVERLOAD_MEMORY_STRESSED": float,
            "OVERLOAD_MEMORY_OVERLOADED": float,
            # Health probe settings (AD-19)
            "LIVENESS_PROBE_TIMEOUT": float,
            "LIVENESS_PROBE_PERIOD": float,
            "LIVENESS_PROBE_FAILURE_THRESHOLD": int,
            "LIVENESS_PROBE_SUCCESS_THRESHOLD": int,
            "READINESS_PROBE_TIMEOUT": float,
            "READINESS_PROBE_PERIOD": float,
            "READINESS_PROBE_FAILURE_THRESHOLD": int,
            "READINESS_PROBE_SUCCESS_THRESHOLD": int,
            "STARTUP_PROBE_TIMEOUT": float,
            "STARTUP_PROBE_PERIOD": float,
            "STARTUP_PROBE_FAILURE_THRESHOLD": int,
            "STARTUP_PROBE_SUCCESS_THRESHOLD": int,
            # Rate limiting settings (AD-24)
            "RATE_LIMIT_DEFAULT_BUCKET_SIZE": int,
            "RATE_LIMIT_DEFAULT_REFILL_RATE": float,
            "RATE_LIMIT_CLIENT_IDLE_TIMEOUT": float,
            "RATE_LIMIT_CLEANUP_INTERVAL": float,
            "RATE_LIMIT_MAX_RETRIES": int,
            "RATE_LIMIT_MAX_TOTAL_WAIT": float,
            "RATE_LIMIT_BACKOFF_MULTIPLIER": float,
            # Healthcheck extension settings (AD-26)
            "EXTENSION_BASE_DEADLINE": float,
            "EXTENSION_MIN_GRANT": float,
            "EXTENSION_MAX_EXTENSIONS": int,
            "EXTENSION_EVICTION_THRESHOLD": int,
            "EXTENSION_EXHAUSTION_WARNING_THRESHOLD": int,
            "EXTENSION_EXHAUSTION_GRACE_PERIOD": float,
            # Orphaned workflow scanner settings
            "ORPHAN_SCAN_INTERVAL": float,
            "ORPHAN_SCAN_WORKER_TIMEOUT": float,
            # Time-windowed stats streaming settings
            "STATS_WINDOW_SIZE_MS": float,
            "STATS_DRIFT_TOLERANCE_MS": float,
            "STATS_PUSH_INTERVAL_MS": float,
            "STATS_MAX_WINDOW_AGE_MS": float,
            "STATUS_UPDATE_POLL_INTERVAL": float,
            "CLIENT_PROGRESS_RATE_LIMIT": float,
            "CLIENT_PROGRESS_BURST": int,
            # Cross-DC correlation settings (Phase 7)
            "CROSS_DC_CORRELATION_WINDOW": float,
            "CROSS_DC_CORRELATION_LOW_THRESHOLD": int,
            "CROSS_DC_CORRELATION_MEDIUM_THRESHOLD": int,
            "CROSS_DC_CORRELATION_HIGH_COUNT_THRESHOLD": int,
            "CROSS_DC_CORRELATION_HIGH_FRACTION": float,
            "CROSS_DC_CORRELATION_BACKOFF": float,
            # Anti-flapping settings
            "CROSS_DC_FAILURE_CONFIRMATION": float,
            "CROSS_DC_RECOVERY_CONFIRMATION": float,
            "CROSS_DC_FLAP_THRESHOLD": int,
            "CROSS_DC_FLAP_DETECTION_WINDOW": float,
            "CROSS_DC_FLAP_COOLDOWN": float,
            # Latency-based correlation settings
            "CROSS_DC_ENABLE_LATENCY_CORRELATION": bool,
            "CROSS_DC_LATENCY_ELEVATED_THRESHOLD_MS": float,
            "CROSS_DC_LATENCY_CRITICAL_THRESHOLD_MS": float,
            "CROSS_DC_MIN_LATENCY_SAMPLES": int,
            "CROSS_DC_LATENCY_SAMPLE_WINDOW": float,
            "CROSS_DC_LATENCY_CORRELATION_FRACTION": float,
            # Extension-based correlation settings
            "CROSS_DC_ENABLE_EXTENSION_CORRELATION": bool,
            "CROSS_DC_EXTENSION_COUNT_THRESHOLD": int,
            "CROSS_DC_EXTENSION_CORRELATION_FRACTION": float,
            "CROSS_DC_EXTENSION_WINDOW": float,
            # LHM-based correlation settings
            "CROSS_DC_ENABLE_LHM_CORRELATION": bool,
            "CROSS_DC_LHM_STRESSED_THRESHOLD": int,
            "CROSS_DC_LHM_CORRELATION_FRACTION": float,
            # Recovery and thundering herd settings
            "RECOVERY_JITTER_MAX": float,
            "RECOVERY_JITTER_MIN": float,
            "RECOVERY_MAX_CONCURRENT": int,
            "RECOVERY_SEMAPHORE_SIZE": int,
            "DISPATCH_MAX_CONCURRENT_PER_WORKER": int,
            "MESSAGE_QUEUE_MAX_SIZE": int,
            "MESSAGE_QUEUE_WARN_SIZE": int,
            # Bounded pending response queues settings (AD-32)
            "PENDING_RESPONSE_MAX_CONCURRENT": int,
            "PENDING_RESPONSE_HIGH_LIMIT": int,
            "PENDING_RESPONSE_NORMAL_LIMIT": int,
            "PENDING_RESPONSE_LOW_LIMIT": int,
            "PENDING_RESPONSE_WARN_THRESHOLD": float,
            # Client-side queue settings (AD-32)
            "OUTGOING_QUEUE_SIZE": int,
            "OUTGOING_OVERFLOW_SIZE": int,
            "OUTGOING_MAX_DESTINATIONS": int,
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
            'refutation_rate_limit_tokens': self.SWIM_REFUTATION_RATE_LIMIT_TOKENS,
            'refutation_rate_limit_window': self.SWIM_REFUTATION_RATE_LIMIT_WINDOW,
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

    def get_overload_config(self):
        """
        Get overload detection configuration (AD-18).

        Creates an OverloadConfig instance from environment settings.
        Uses hybrid detection combining delta-based, absolute bounds,
        and resource-based (CPU/memory) signals.
        """
        from hyperscale.distributed_rewrite.reliability.overload import OverloadConfig

        return OverloadConfig(
            ema_alpha=self.OVERLOAD_EMA_ALPHA,
            current_window=self.OVERLOAD_CURRENT_WINDOW,
            trend_window=self.OVERLOAD_TREND_WINDOW,
            min_samples=self.OVERLOAD_MIN_SAMPLES,
            trend_threshold=self.OVERLOAD_TREND_THRESHOLD,
            delta_thresholds=(
                self.OVERLOAD_DELTA_BUSY,
                self.OVERLOAD_DELTA_STRESSED,
                self.OVERLOAD_DELTA_OVERLOADED,
            ),
            absolute_bounds=(
                self.OVERLOAD_ABSOLUTE_BUSY_MS,
                self.OVERLOAD_ABSOLUTE_STRESSED_MS,
                self.OVERLOAD_ABSOLUTE_OVERLOADED_MS,
            ),
            cpu_thresholds=(
                self.OVERLOAD_CPU_BUSY,
                self.OVERLOAD_CPU_STRESSED,
                self.OVERLOAD_CPU_OVERLOADED,
            ),
            memory_thresholds=(
                self.OVERLOAD_MEMORY_BUSY,
                self.OVERLOAD_MEMORY_STRESSED,
                self.OVERLOAD_MEMORY_OVERLOADED,
            ),
        )

    def get_liveness_probe_config(self):
        """
        Get liveness probe configuration (AD-19).

        Liveness probes check if the process is running and responsive.
        Failure triggers restart/replacement.
        """
        from hyperscale.distributed_rewrite.health.probes import ProbeConfig

        return ProbeConfig(
            timeout_seconds=self.LIVENESS_PROBE_TIMEOUT,
            period_seconds=self.LIVENESS_PROBE_PERIOD,
            failure_threshold=self.LIVENESS_PROBE_FAILURE_THRESHOLD,
            success_threshold=self.LIVENESS_PROBE_SUCCESS_THRESHOLD,
        )

    def get_readiness_probe_config(self):
        """
        Get readiness probe configuration (AD-19).

        Readiness probes check if the node can accept work.
        Failure removes from load balancer/routing.
        """
        from hyperscale.distributed_rewrite.health.probes import ProbeConfig

        return ProbeConfig(
            timeout_seconds=self.READINESS_PROBE_TIMEOUT,
            period_seconds=self.READINESS_PROBE_PERIOD,
            failure_threshold=self.READINESS_PROBE_FAILURE_THRESHOLD,
            success_threshold=self.READINESS_PROBE_SUCCESS_THRESHOLD,
        )

    def get_startup_probe_config(self):
        """
        Get startup probe configuration (AD-19).

        Startup probes check if initialization is complete.
        Delays liveness/readiness until startup complete.
        """
        from hyperscale.distributed_rewrite.health.probes import ProbeConfig

        return ProbeConfig(
            timeout_seconds=self.STARTUP_PROBE_TIMEOUT,
            period_seconds=self.STARTUP_PROBE_PERIOD,
            failure_threshold=self.STARTUP_PROBE_FAILURE_THRESHOLD,
            success_threshold=self.STARTUP_PROBE_SUCCESS_THRESHOLD,
        )

    def get_rate_limit_config(self):
        """
        Get rate limiting configuration (AD-24).

        Creates a RateLimitConfig with default bucket settings.
        Per-operation limits can be customized after creation.
        """
        from hyperscale.distributed_rewrite.reliability.rate_limiting import RateLimitConfig

        return RateLimitConfig(
            default_bucket_size=self.RATE_LIMIT_DEFAULT_BUCKET_SIZE,
            default_refill_rate=self.RATE_LIMIT_DEFAULT_REFILL_RATE,
        )

    def get_rate_limit_retry_config(self):
        """
        Get rate limit retry configuration (AD-24).

        Controls how clients retry after being rate limited.
        """
        from hyperscale.distributed_rewrite.reliability.rate_limiting import RateLimitRetryConfig

        return RateLimitRetryConfig(
            max_retries=self.RATE_LIMIT_MAX_RETRIES,
            max_total_wait=self.RATE_LIMIT_MAX_TOTAL_WAIT,
            backoff_multiplier=self.RATE_LIMIT_BACKOFF_MULTIPLIER,
        )

    def get_worker_health_manager_config(self):
        """
        Get worker health manager configuration (AD-26).

        Controls deadline extension tracking for workers.
        Extensions use logarithmic decay to prevent indefinite extensions.
        """
        from hyperscale.distributed_rewrite.health.worker_health_manager import (
            WorkerHealthManagerConfig,
        )

        return WorkerHealthManagerConfig(
            base_deadline=self.EXTENSION_BASE_DEADLINE,
            min_grant=self.EXTENSION_MIN_GRANT,
            max_extensions=self.EXTENSION_MAX_EXTENSIONS,
            eviction_threshold=self.EXTENSION_EVICTION_THRESHOLD,
        )

    def get_extension_tracker_config(self):
        """
        Get extension tracker configuration (AD-26).

        Creates configuration for per-worker extension trackers.
        """
        from hyperscale.distributed_rewrite.health.extension_tracker import (
            ExtensionTrackerConfig,
        )

        return ExtensionTrackerConfig(
            base_deadline=self.EXTENSION_BASE_DEADLINE,
            min_grant=self.EXTENSION_MIN_GRANT,
            max_extensions=self.EXTENSION_MAX_EXTENSIONS,
        )

    def get_cross_dc_correlation_config(self):
        """
        Get cross-DC correlation configuration (Phase 7).

        Controls cascade eviction prevention when multiple DCs fail
        simultaneously (likely network partition, not actual DC failures).

        HIGH correlation requires BOTH:
        - Fraction of DCs >= high_threshold_fraction (e.g., 50%)
        - Count of DCs >= high_count_threshold (e.g., 4)

        This prevents false positives when few DCs exist.

        Anti-flapping mechanisms:
        - Failure confirmation: failures must persist before counting
        - Recovery confirmation: recovery must be sustained before healthy
        - Flap detection: too many state changes marks DC as flapping

        Secondary correlation signals:
        - Latency correlation: elevated latency across DCs = network issue
        - Extension correlation: many extensions across DCs = load spike
        - LHM correlation: high LHM scores across DCs = systemic stress
        """
        from hyperscale.distributed_rewrite.datacenters.cross_dc_correlation import (
            CrossDCCorrelationConfig,
        )

        return CrossDCCorrelationConfig(
            # Primary thresholds
            correlation_window_seconds=self.CROSS_DC_CORRELATION_WINDOW,
            low_threshold=self.CROSS_DC_CORRELATION_LOW_THRESHOLD,
            medium_threshold=self.CROSS_DC_CORRELATION_MEDIUM_THRESHOLD,
            high_count_threshold=self.CROSS_DC_CORRELATION_HIGH_COUNT_THRESHOLD,
            high_threshold_fraction=self.CROSS_DC_CORRELATION_HIGH_FRACTION,
            correlation_backoff_seconds=self.CROSS_DC_CORRELATION_BACKOFF,
            # Anti-flapping
            failure_confirmation_seconds=self.CROSS_DC_FAILURE_CONFIRMATION,
            recovery_confirmation_seconds=self.CROSS_DC_RECOVERY_CONFIRMATION,
            flap_threshold=self.CROSS_DC_FLAP_THRESHOLD,
            flap_detection_window_seconds=self.CROSS_DC_FLAP_DETECTION_WINDOW,
            flap_cooldown_seconds=self.CROSS_DC_FLAP_COOLDOWN,
            # Latency-based correlation
            enable_latency_correlation=self.CROSS_DC_ENABLE_LATENCY_CORRELATION,
            latency_elevated_threshold_ms=self.CROSS_DC_LATENCY_ELEVATED_THRESHOLD_MS,
            latency_critical_threshold_ms=self.CROSS_DC_LATENCY_CRITICAL_THRESHOLD_MS,
            min_latency_samples=self.CROSS_DC_MIN_LATENCY_SAMPLES,
            latency_sample_window_seconds=self.CROSS_DC_LATENCY_SAMPLE_WINDOW,
            latency_correlation_fraction=self.CROSS_DC_LATENCY_CORRELATION_FRACTION,
            # Extension-based correlation
            enable_extension_correlation=self.CROSS_DC_ENABLE_EXTENSION_CORRELATION,
            extension_count_threshold=self.CROSS_DC_EXTENSION_COUNT_THRESHOLD,
            extension_correlation_fraction=self.CROSS_DC_EXTENSION_CORRELATION_FRACTION,
            extension_window_seconds=self.CROSS_DC_EXTENSION_WINDOW,
            # LHM-based correlation
            enable_lhm_correlation=self.CROSS_DC_ENABLE_LHM_CORRELATION,
            lhm_stressed_threshold=self.CROSS_DC_LHM_STRESSED_THRESHOLD,
            lhm_correlation_fraction=self.CROSS_DC_LHM_CORRELATION_FRACTION,
        )

    def get_discovery_config(
        self,
        cluster_id: str = "hyperscale",
        environment_id: str = "default",
        node_role: str = "worker",
        static_seeds: list[str] | None = None,
        allow_dynamic_registration: bool = False,
    ):
        """
        Get discovery service configuration (AD-28).

        Creates configuration for peer discovery, locality-aware selection,
        and adaptive load balancing.

        Args:
            cluster_id: Cluster identifier for filtering peers
            environment_id: Environment identifier
            node_role: Role of the local node ('worker', 'manager', etc.)
            static_seeds: Static seed addresses in "host:port" format
            allow_dynamic_registration: Allow empty seeds (peers register dynamically)
        """
        from hyperscale.distributed_rewrite.discovery.models.discovery_config import (
            DiscoveryConfig,
        )

        # Parse DNS names from comma-separated string
        dns_names: list[str] = []
        if self.DISCOVERY_DNS_NAMES:
            dns_names = [name.strip() for name in self.DISCOVERY_DNS_NAMES.split(",") if name.strip()]

        # Parse allowed CIDRs from comma-separated string
        dns_allowed_cidrs: list[str] = []
        if self.DISCOVERY_DNS_ALLOWED_CIDRS:
            dns_allowed_cidrs = [
                cidr.strip()
                for cidr in self.DISCOVERY_DNS_ALLOWED_CIDRS.split(",")
                if cidr.strip()
            ]

        return DiscoveryConfig(
            cluster_id=cluster_id,
            environment_id=environment_id,
            node_role=node_role,
            dns_names=dns_names,
            static_seeds=static_seeds or [],
            default_port=self.DISCOVERY_DEFAULT_PORT,
            dns_cache_ttl=self.DISCOVERY_DNS_CACHE_TTL,
            dns_timeout=self.DISCOVERY_DNS_TIMEOUT,
            # DNS Security settings
            dns_allowed_cidrs=dns_allowed_cidrs,
            dns_block_private_for_public=self.DISCOVERY_DNS_BLOCK_PRIVATE_FOR_PUBLIC,
            dns_detect_ip_changes=self.DISCOVERY_DNS_DETECT_IP_CHANGES,
            dns_max_ip_changes_per_window=self.DISCOVERY_DNS_MAX_IP_CHANGES,
            dns_ip_change_window_seconds=self.DISCOVERY_DNS_IP_CHANGE_WINDOW,
            dns_reject_on_security_violation=self.DISCOVERY_DNS_REJECT_ON_VIOLATION,
            # Locality settings
            datacenter_id=self.DISCOVERY_DATACENTER_ID,
            region_id=self.DISCOVERY_REGION_ID,
            prefer_same_dc=self.DISCOVERY_PREFER_SAME_DC,
            candidate_set_size=self.DISCOVERY_CANDIDATE_SET_SIZE,
            ewma_alpha=self.DISCOVERY_EWMA_ALPHA,
            baseline_latency_ms=self.DISCOVERY_BASELINE_LATENCY_MS,
            latency_multiplier_threshold=self.DISCOVERY_LATENCY_MULTIPLIER_THRESHOLD,
            min_peers_per_tier=self.DISCOVERY_MIN_PEERS_PER_TIER,
            max_concurrent_probes=self.DISCOVERY_MAX_CONCURRENT_PROBES,
            # Dynamic registration mode
            allow_dynamic_registration=allow_dynamic_registration,
        )

    def get_pending_response_config(self) -> dict:
        """
        Get bounded pending response configuration (AD-32).

        Returns configuration for the priority-aware bounded execution system:
        - Per-priority limits (CRITICAL unlimited, HIGH/NORMAL/LOW bounded)
        - Global limit across all priorities
        - Load shedding: LOW shed first, then NORMAL, then HIGH
        - CRITICAL (SWIM probes/acks) NEVER shed

        This prevents memory exhaustion under high load while:
        - Ensuring SWIM protocol accuracy (CRITICAL never delayed)
        - Providing graceful degradation (shed stats before job commands)
        - Enabling immediate execution (no queue latency for most messages)
        """
        return {
            'global_limit': self.PENDING_RESPONSE_MAX_CONCURRENT,
            'high_limit': self.PENDING_RESPONSE_HIGH_LIMIT,
            'normal_limit': self.PENDING_RESPONSE_NORMAL_LIMIT,
            'low_limit': self.PENDING_RESPONSE_LOW_LIMIT,
            'warn_threshold': self.PENDING_RESPONSE_WARN_THRESHOLD,
        }

    def get_outgoing_queue_config(self) -> dict:
        """
        Get client-side outgoing queue configuration (AD-32).

        Returns configuration for per-destination RobustMessageQueue:
        - Per-destination queue isolation (slow DC doesn't block fast DC)
        - Graduated backpressure (HEALTHY → THROTTLED → BATCHING → OVERFLOW)
        - LRU eviction when max destinations reached
        """
        return {
            'queue_size': self.OUTGOING_QUEUE_SIZE,
            'overflow_size': self.OUTGOING_OVERFLOW_SIZE,
            'max_destinations': self.OUTGOING_MAX_DESTINATIONS,
        }
