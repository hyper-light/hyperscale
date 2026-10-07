from __future__ import annotations
import os
import orjson
from pydantic import BaseModel, StrictBool, StrictStr, StrictInt, StrictFloat
from typing import Callable, Dict, Literal, Union

PrimaryType = Union[str, int, float, bytes, bool]


TRUE_ENVAR_WORDS = frozenset({"y", "yes", "t", "true", "on", "1"})
FALSE_ENVAR_WORDS = frozenset({"n", "no", "f", "false", "off", "0"})


def parse_bool_envar(value: str) -> bool:
    """
    Parse a boolean environment variable (``bool("false")`` is True).

    Accepts the words the standard library's ``strtobool`` accepted, in
    any case; anything else is refused rather than read as False, so a
    misspelled setting stops the node instead of silently disabling it.

    Raises:
        ValueError: ``value`` is not a boolean word.
    """
    word = value.strip().lower()
    if word in TRUE_ENVAR_WORDS:
        return True

    if word in FALSE_ENVAR_WORDS:
        return False

    raise ValueError(
        f"invalid boolean {value!r}: expected one of "
        f"{sorted(TRUE_ENVAR_WORDS | FALSE_ENVAR_WORDS)}"
    )


class Env(BaseModel):
    MERCURY_SYNC_CONNECT_SECONDS: StrictStr = "30s"
    MERCURY_SYNC_SERVER_URL: StrictStr | None = None
    MERCURY_SYNC_API_VERISON: StrictStr = "0.0.1"
    MERCURY_SYNC_TASK_EXECUTOR_TYPE: Literal["thread", "process", "none"] = "thread"
    MERCURY_SYNC_TCP_CONNECT_RETRIES: StrictInt = 3
    MERCURY_SYNC_UDP_CONNECT_RETRIES: StrictInt = 3
    MERCURY_SYNC_CLEANUP_INTERVAL: StrictStr = "0.25s"
    MERCURY_SYNC_MAX_CONCURRENCY: StrictInt = 4096
    # No default: a published default would let anyone who read it run code
    # on an unconfigured cluster. Every node must be given one strong, random
    # secret (MERCURY_SYNC_AUTH_SECRET or --acm-secret); the encryptor refuses
    # None, short and known-weak values.
    MERCURY_SYNC_AUTH_SECRET: StrictStr | None = None
    MERCURY_SYNC_AUTH_SECRET_PREVIOUS: StrictStr | None = None
    MERCURY_SYNC_LOGS_DIRECTORY: StrictStr = os.getcwd()
    MERCURY_SYNC_REQUEST_TIMEOUT: StrictStr = "30s"
    MERCURY_SYNC_LOG_LEVEL: StrictStr = "info"
    MERCURY_SYNC_TASK_RUNNER_MAX_THREADS: StrictInt = os.cpu_count() or 1
    MERCURY_SYNC_TASK_RUNNER_KEEP: StrictInt = 100
    MERCURY_SYNC_MAX_REQUEST_CACHE_SIZE: StrictInt = 100
    MERCURY_SYNC_ENABLE_REQUEST_CACHING: StrictBool = False
    MERCURY_SYNC_TCP_SERVER_BACKLOG: StrictInt = 4096
    # Connections a node's TCP server holds at once (connection-storm cap).
    # 0 sizes it from the process's descriptor soft limit (RLIMIT_NOFILE):
    # half of it -- nodes form a symmetric mesh, each dialing the peers it
    # accepts, so accepted and dialed connections share the descriptors
    # evenly. No cap where the platform reports no limit (Windows, or an
    # unlimited soft limit).
    MERCURY_SYNC_MAX_ACCEPTED_TCP_CONNECTIONS: StrictInt = 0
    MERCURY_SYNC_UDP_SERVER_RCVBUF: StrictInt = 4 * 1024 * 1024
    MERCURY_SYNC_VERIFY_SSL_CERT: Literal["REQUIRED", "OPTIONAL", "NONE"] = "REQUIRED"
    # Secure by default: peer certificates are checked against the
    # connected host. Set to "false" only for local certs without
    # matching SAN entries.
    MERCURY_SYNC_TLS_VERIFY_HOSTNAME: StrictStr = "true"
    # Peers addressed by DNS name (a Kubernetes pod's stable name) are sent
    # datagrams at the name's resolved IP, cached this many seconds: the
    # TTL CoreDNS serves Kubernetes records with by default, so a restarted
    # pod's new IP is used no later than DNS itself would hand it out.
    MERCURY_SYNC_HOST_RESOLUTION_TTL: StrictFloat = 5.0
    # Seconds one lookup may take: the system resolver's own per-attempt
    # default (resolv.conf timeout:5). A failed refresh keeps serving the
    # last known address.
    MERCURY_SYNC_HOST_RESOLUTION_TIMEOUT: StrictFloat = 5.0

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
    SWIM_SUSPICION_MIN_TIMEOUT: StrictFloat = (
        1.5  # Reduced from 2.0 - faster confirmation
    )
    SWIM_SUSPICION_MAX_TIMEOUT: StrictFloat = (
        8.0  # Reduced from 15.0 - faster failure declaration
    )
    SWIM_NO_WITNESS_SUSPICION_TIMEOUT: StrictFloat = 30.0
    # Suspicion bounds of a manager's job layer (the hierarchical detector's
    # per-job view of its workers), beside the global bounds above.
    MANAGER_SWIM_JOB_MIN_TIMEOUT: StrictFloat = 2.0
    MANAGER_SWIM_JOB_MAX_TIMEOUT: StrictFloat = 15.0
    # AD-53 burst-failure cluster-degradation signal.
    # When the prober observes ``BURST_FAILURE_THRESHOLD`` distinct
    # direct+indirect probe failures within
    # ``BURST_FAILURE_WINDOW_SECONDS``, it temporarily widens SWIM
    # confirmation work for other silent members. Each accelerated
    # target still runs direct probe -> indirect probe -> SUSPECT; the
    # burst signal changes probe scheduling pressure, not membership
    # state semantics. The window has to be wide enough to absorb
    # LHM-stretched probe rounds (which can run at base_timeout ×
    # lhm_max ≈ 9 s per direct probe, plus indirect) under a
    # bulk-failure burst. See AD-53.
    BURST_FAILURE_THRESHOLD: StrictInt = 2
    BURST_FAILURE_WINDOW_SECONDS: StrictFloat = 30.0
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
    LEADER_MAX_LHM: StrictInt = (
        4  # Max LHM score for leader eligibility (higher = more tolerant)
    )

    # Job Lease Settings (Gate per-job ownership)
    JOB_LEASE_DURATION: StrictFloat = 30.0  # Duration of job ownership lease in seconds
    JOB_LEASE_CLEANUP_INTERVAL: StrictFloat = (
        10.0  # How often to clean up expired job leases
    )

    IDEMPOTENCY_PENDING_TTL_SECONDS: StrictFloat = 60.0
    IDEMPOTENCY_COMMITTED_TTL_SECONDS: StrictFloat = 300.0
    IDEMPOTENCY_REJECTED_TTL_SECONDS: StrictFloat = 60.0
    IDEMPOTENCY_MAX_ENTRIES: StrictInt = 100_000
    IDEMPOTENCY_CLEANUP_INTERVAL_SECONDS: StrictFloat = 10.0
    IDEMPOTENCY_WAIT_FOR_PENDING: StrictBool = True
    IDEMPOTENCY_PENDING_WAIT_TIMEOUT: StrictFloat = 30.0

    # Cluster Formation Settings

    CLUSTER_STABILIZATION_TIMEOUT: StrictFloat = (
        10.0  # Max seconds to wait for cluster to form
    )
    CLUSTER_STABILIZATION_POLL_INTERVAL: StrictFloat = (
        0.5  # How often to check cluster membership
    )
    LEADER_ELECTION_JITTER_MAX: StrictFloat = (
        3.0  # Max random delay before starting first election
    )
    # AD-52 slice C, the cluster membership group. A formation round
    # greets every founder once: one per SWIM probe period
    # (SWIM_UDP_POLL_INTERVAL), the per-peer cadence the failure detector
    # already pays for. A membership claim that has not committed within a
    # round is retried in the next.
    CLUSTER_FORMATION_INTERVAL_SECONDS: StrictFloat = 1.0
    # AD-52 section 8: a member the membership group's leader has not heard
    # from for this long no longer holds the cluster back -- its address is
    # released, and a cluster unable to commit for this long is founded
    # anew. Minutes, not seconds: brief partitions are common and real
    # death rare, and service registries that evict on a blip cause
    # outages (Linkerd and Istio hold endpoints for minutes). A process
    # restarted at its address takes its place at once, without waiting.
    CLUSTER_TOMBSTONE_RETENTION_SECONDS: StrictFloat = 600.0
    # AD-52 section 9: a membership watch is a long poll a member answers at
    # once when membership changed, else after this long with nothing new.
    # With the request's own transport budget (MANAGER/GATE_TCP_TIMEOUT_
    # STANDARD, 5s) it stays under 30s -- the shortest common proxy request
    # timeout (Google Cloud load balancer backend default 30s; AWS ALB idle
    # 60s) -- so a watch through one is never cut; longer polls only save
    # an idle request per wait.
    CLUSTER_WATCH_WAIT_SECONDS: StrictFloat = 25.0
    # AD-52 section 7: the membership group snapshots its state once this many
    # entries were applied since its last snapshot, and keeps the
    # CLUSTER_SNAPSHOT_CATCH_UP_ENTRIES before the snapshot point in its log:
    # a member or watcher that far behind catches up from the log, one
    # further behind is sent the snapshot. etcd's defaults (snapshot count
    # 10,000 before v3.2 -- 100,000 after, above RaftLog's 50,000 cap -- and
    # 5,000 catch-up entries); together they stay under that cap.
    CLUSTER_SNAPSHOT_ENTRIES: StrictInt = 10_000
    CLUSTER_SNAPSHOT_CATCH_UP_ENTRIES: StrictInt = 5_000
    # D1: a Raft store found untrustworthy at start is set aside, never
    # deleted unread, and this many of the newest set-asides are kept. The
    # newest describes the failure an operator investigates -- older ones
    # are superseded by it -- and keeping every one would let a disk that
    # fails at each start fill itself.
    RAFT_SET_ASIDE_RETAINED: StrictInt = 1
    # Leader leases (AD-52 section 11; Raft thesis 6.4.1): a leader a quorum
    # answered serves linearizable reads without a round of its own. Opt-in
    # -- they assume every member's clock RATE stays within
    # RAFT_CLOCK_DRIFT_BOUND of the others' (offsets do not matter); off,
    # every ReadIndex pays one round, with no clock assumption. Every member
    # of a cluster must agree on the setting.
    RAFT_LEADER_LEASES_ENABLED: StrictBool = False
    # The largest relative clock-rate error a lease tolerates: 500 ppm, the
    # most frequency error NTP's clock discipline corrects (RFC 5905,
    # MAXFREQ) -- the bound an NTP-synchronized host keeps.
    RAFT_CLOCK_DRIFT_BOUND: StrictFloat = 500e-6

    # Federated Health Monitor Settings (Gate -> DC Leader probing)
    # These are tuned for high-latency, globally distributed links
    FEDERATED_PROBE_INTERVAL: StrictFloat = 2.0  # Seconds between probes to each DC
    FEDERATED_PROBE_TIMEOUT: StrictFloat = (
        5.0  # Timeout for single probe (high for cross-DC)
    )
    FEDERATED_SUSPICION_TIMEOUT: StrictFloat = (
        30.0  # Time before suspected -> unreachable
    )
    FEDERATED_MAX_CONSECUTIVE_FAILURES: StrictInt = (
        5  # Failures before marking suspected
    )

    # Circuit Breaker Settings
    CIRCUIT_BREAKER_MAX_ERRORS: StrictInt = 3
    CIRCUIT_BREAKER_WINDOW_SECONDS: StrictFloat = 30.0
    CIRCUIT_BREAKER_HALF_OPEN_AFTER: StrictFloat = 10.0

    # Worker Progress Update Settings (tuned for real-time terminal UI)
    WORKER_PROGRESS_UPDATE_INTERVAL: StrictFloat = (
        0.05  # How often to collect progress locally (50ms)
    )
    WORKER_PROGRESS_FLUSH_INTERVAL: StrictFloat = (
        0.05  # How often to send buffered updates to manager (50ms)
    )
    WORKER_MAX_CORES: StrictInt | None = None

    # Worker Dead Manager Cleanup Settings
    WORKER_DEAD_MANAGER_REAP_INTERVAL: StrictFloat = (
        900.0  # Seconds before reaping dead managers (15 minutes)
    )
    WORKER_DEAD_MANAGER_CHECK_INTERVAL: StrictFloat = (
        60.0  # Seconds between dead manager checks
    )

    # Worker Cluster-Connection Liveness Settings.
    # ``WorkerClusterConnection`` runs a periodic watchdog that
    # downgrades a manager to unhealthy when its last heartbeat is
    # older than the staleness threshold. The watchdog catches the
    # "address stays alive but identity changed" case (kill-and-
    # restart at the same UDP/TCP port) that SWIM's address-keyed
    # probe cannot see. Default sized for ≥3 SWIM protocol periods
    # plus headroom for jitter under load — tighten for faster
    # recovery only when the cluster is otherwise quiescent.
    WORKER_CLUSTER_LIVENESS_CHECK_INTERVAL: StrictFloat = (
        2.0  # Seconds between staleness scans
    )
    WORKER_CLUSTER_HEARTBEAT_STALENESS_THRESHOLD: StrictFloat = (
        20.0  # Seconds a manager may go silent before being downgraded
    )
    WORKER_CLUSTER_REJOIN_BASE_BACKOFF: StrictFloat = (
        2.0  # Base inter-pass backoff for rejoin loop (LHM-scaled)
    )

    # Worker Cancellation Polling Settings
    WORKER_CANCELLATION_POLL_INTERVAL: StrictFloat = (
        5.0  # Seconds between cancellation poll requests
    )

    # Worker Load-Sampling Settings, consumed by ``WorkerConfig.from_env``.
    WORKER_OVERLOAD_POLL_INTERVAL: StrictFloat = (
        0.25  # Seconds between CPU/overload samples
    )
    WORKER_THROUGHPUT_INTERVAL_SECONDS: StrictFloat = (
        10.0  # Seconds between throughput-rate samples
    )

    # Worker Backpressure Delay Settings (AD-37)
    WORKER_BACKPRESSURE_THROTTLE_DELAY_MS: StrictInt = 500  # Default THROTTLE delay
    WORKER_BACKPRESSURE_BATCH_DELAY_MS: StrictInt = 1000  # Default BATCH delay
    WORKER_BACKPRESSURE_REJECT_DELAY_MS: StrictInt = 2000  # Default REJECT delay

    # Worker TCP Timeout Settings
    WORKER_TCP_TIMEOUT_SHORT: StrictFloat = 2.0  # Short timeout for quick operations
    WORKER_TCP_TIMEOUT_STANDARD: StrictFloat = (
        5.0  # Standard timeout for progress/result pushes
    )
    # Seconds a progress send to a job leader may take: the next progress
    # update supersedes a late one.
    WORKER_PROGRESS_SEND_TIMEOUT: StrictFloat = 1.0
    # Seconds a heartbeat send to a manager may take.
    WORKER_HEARTBEAT_SEND_TIMEOUT: StrictFloat = 1.0
    # Seconds between cancellation checks while a running workflow's next
    # status update is awaited.
    WORKER_EXECUTION_UPDATE_WAIT: StrictFloat = 0.5
    # Seconds a worker waits for a cancelled workflow to stop.
    WORKER_WORKFLOW_CANCEL_TIMEOUT: StrictFloat = 5.0
    # Final results a job leader has not acknowledged: at most
    # WORKER_PENDING_RESULT_LIMIT are kept, each resent with exponential
    # backoff from WORKER_RESULT_RETRY_BASE_DELAY up to
    # WORKER_RESULT_MAX_RETRIES times -- attempts made only while a manager
    # is reachable, so an isolated worker keeps its results until it can
    # deliver them (AD-52 section 10).
    WORKER_PENDING_RESULT_LIMIT: StrictInt = 1000
    WORKER_RESULT_MAX_RETRIES: StrictInt = 10
    WORKER_RESULT_RETRY_BASE_DELAY: StrictFloat = 5.0
    # Recent workflow completion times a worker keeps for its throughput
    # estimate (AD-19).
    WORKER_COMPLETION_TIMES_MAX_SAMPLES: StrictInt = 50
    WORKER_REGISTRATION_MAX_RETRIES: StrictInt = 5
    WORKER_REGISTRATION_BASE_DELAY: StrictFloat = 0.25
    # Wait a random jittered delay before this worker's *first* register
    # attempt to spread the cold-start register storm. The default has
    # to absorb large clusters: with N workers and jitter ``J`` the
    # effective per-second register rate seen by the manager is ``N/J``;
    # at the previous default of 0.25s an N=50 cluster generates ~200
    # connects/second, which exceeds the macOS/Linux per-socket accept
    # drain rate and causes ECONNREFUSED + per-worker circuit-breaker
    # open + permanent registration failure. 5s keeps the rate under
    # ~10/sec for N≤50 and degrades gracefully past that. Single-digit
    # cold-start latency is acceptable because workers retry with their
    # own backoff after the initial attempt.
    WORKER_INITIAL_REGISTRATION_JITTER_MAX: StrictFloat = 5.0

    # Time budget for the worker's local subprocess pool to spawn and
    # acknowledge readiness. *Distinct* from
    # ``MERCURY_SYNC_CONNECT_SECONDS``: that knob is the network
    # connect timeout for an *existing* peer (UDP/TCP socket setup,
    # measured in milliseconds), while this one covers a full
    # ``spawn`` of a fresh Python interpreter plus importing the
    # hyperscale package — which on macOS/Linux is on the order of
    # 1-2 seconds per subprocess in isolation and substantially more
    # under concurrent startup (cold disk cache, fork lock contention,
    # interpreter init serialization). The previous code reused
    # ``MERCURY_SYNC_CONNECT_SECONDS + 10s`` here, which works in
    # production where each worker is in its own container but is
    # too tight whenever many workers are launched on the same host
    # at the same time (simulation harnesses, dev VMs, k8s pods
    # scheduled densely onto one node). Default is generous enough
    # to absorb that load without operator tuning.
    WORKER_POOL_STARTUP_TIMEOUT_SECONDS: StrictFloat = 60.0

    # Worker orphan handling (Section 2.7). The grace a workflow whose job
    # leader died waits for its new leader is derived from the cluster's
    # own timings (nodes/worker/worker_config_derivation.py derive_orphan_grace_seconds),
    # learned from the rescues the worker observes, and extended (AD-26)
    # while managers keep heartbeating it.
    WORKER_ORPHAN_CHECK_INTERVAL: StrictFloat = (
        1.0  # Seconds between orphan grace period checks
    )

    # Worker Job Leadership Transfer Settings (Section 8)
    # TTL for pending transfers that arrive before workflows are known
    WORKER_PENDING_TRANSFER_TTL: StrictFloat = (
        60.0  # Seconds to retain pending transfers
    )

    # Manager Startup and Dispatch Settings
    MANAGER_STARTUP_SYNC_DELAY: StrictFloat = (
        2.0  # Seconds to wait for leader election before state sync
    )
    MANAGER_STATE_SYNC_TIMEOUT: StrictFloat = (
        5.0  # Timeout for state sync request to leader
    )
    MANAGER_STATE_SYNC_RETRIES: StrictInt = 3  # Number of retries for state sync
    MANAGER_DISPATCH_CORE_WAIT_TIMEOUT: StrictFloat = (
        5.0  # Max seconds to wait per iteration for cores
    )
    # Seconds between manager heartbeats to gates. A gate's datacenter
    # health, AD-41 resource view and AD-43 capacity view each go stale
    # after 30s without one: 5s keeps six heartbeats in that window (the
    # Kubernetes node heartbeat defaults keep four, 10s against a 40s
    # grace period), so a few lost or late sends never stale a healthy
    # datacenter, and placement sees storage, load and capacity changes
    # within 5s.
    MANAGER_HEARTBEAT_INTERVAL: StrictFloat = 5.0
    MAX_WORKERS_PER_MANAGER: StrictInt | None = None
    """Optional hard cap for worker registrations accepted by a manager.

    ``None`` preserves the historical unlimited behavior. Tests and
    deployments can set this to a non-negative integer when they need a
    bounded worker fan-in per manager.
    """
    # D-65: concurrency caps, enforced by the datacenter's leader manager at
    # job admission -- where a gateless deployment's submissions arrive too.
    # A capped submission is refused with ``JobAck.retry_after_seconds``.
    #
    # Unset (None), a datacenter admits a job while the work of its
    # unfinished jobs plus the new job's fits its registered cores over the
    # new job's timeout:
    #     sum(core_seconds of unfinished jobs) + core_seconds(new) <= C * timeout(new)
    # where C is the cores of the datacenter's registered workers and a
    # job's core_seconds is, per workflow, min(C, max(1, vus)) cores (the
    # per-workflow core demand the dispatcher gives an AUTO workflow) times
    # its duration. Past that, the datacenter's cores -- fully busy -- cannot
    # finish the job inside its own timeout: admitting it accepted a job
    # bound to time out (AD-34) instead of telling the submitter to come back.
    # Set, at most this many unfinished jobs at once, whatever their size.
    JOB_CONCURRENCY_CAP_PER_DC: StrictInt | None = None
    # D-65: per job class caps -- "<class>=<count>" entries, comma-separated;
    # a class is its workflows' names joined by "+" ("Setup+Load=2"). A class
    # not named here is held only by the datacenter's cap: the derived
    # per-class cap -- the same work-over-timeout rule over the class's own
    # jobs -- never binds before the datacenter-wide one does.
    JOB_CLASS_CONCURRENCY_CAPS: StrictStr = ""
    MANAGER_PEER_SYNC_INTERVAL: StrictFloat = (
        10.0  # Seconds between re-registrations with peer managers that missed a rejoin
    )
    # Seconds between full job-state syncs from each job's leader to its
    # peer managers. A backstop: submission, dispatch, takeover and
    # cleanup sync at once (with quorum where it matters). Full-state
    # anti-entropy elsewhere runs every 15-60s (memberlist 15/30/60s by
    # network, Consul 60s); its cost grows with jobs x peers.
    MANAGER_PEER_JOB_SYNC_INTERVAL: StrictFloat = 15.0

    # Job Cleanup Settings. A completed job stays in memory for a while, so
    # a manager without a ledger acks a late result stale (after cleanup it
    # refuses it, and the worker's attempt budget settles it); with a
    # ledger, status and late results are answered from the ledger after
    # cleanup. Failed, cancelled and
    # timed-out jobs stay an hour for investigation.
    COMPLETED_JOB_MAX_AGE: StrictFloat = 300.0
    FAILED_JOB_MAX_AGE: StrictFloat = 3600.0
    # Seconds between job cleanup sweeps. The sweep also reconciles a copy of
    # a job led elsewhere that heard no sync for a whole interval; its leader
    # re-syncs every MANAGER_PEER_JOB_SYNC_INTERVAL (15 s), so 60 s spans four
    # syncs and tolerates three consecutive lost ones before asking (the
    # missed-heartbeat multiple failure detectors use) while retention ends
    # within one interval (20%) of COMPLETED_JOB_MAX_AGE.
    JOB_CLEANUP_INTERVAL: StrictFloat = 60.0

    # AD-41 resource guards: the per-job budget a workflow is enforced
    # against when the job assigns none, and the graduated-response
    # thresholds/graces (values from the AD-41 ResourceBudget spec).
    RESOURCE_GUARD_ENABLED: StrictBool = True
    RESOURCE_GUARD_MAX_CPU_PERCENT: StrictFloat = 800.0
    RESOURCE_GUARD_MAX_MEMORY_BYTES: StrictInt = 16 * 1024 * 1024 * 1024
    RESOURCE_GUARD_WARNING_THRESHOLD: StrictFloat = 0.8
    # AD-41 THROTTLE: past this fraction of its budget a workflow's
    # concurrency is cut back toward it (the spec's 85%).
    RESOURCE_GUARD_THROTTLE_THRESHOLD: StrictFloat = 0.85
    RESOURCE_GUARD_KILL_THRESHOLD: StrictFloat = 1.0
    RESOURCE_GUARD_WARNING_GRACE_SECONDS: StrictFloat = 10.0
    RESOURCE_GUARD_KILL_GRACE_SECONDS: StrictFloat = 2.0
    # AD-41 resource views: a workflow's estimate (on its job leader) or a
    # manager's report (on a gate) older than this no longer counts
    # toward the datacenter's resource pressure (AD-41 "30s threshold").
    RESOURCE_VIEW_STALENESS_SECONDS: StrictFloat = 30.0

    # AD-39 hybrid logical clock offset bound (epsilon): a timestamp more
    # than this far ahead of a node's physical clock is refused rather
    # than adopted. 500ms is CockroachDB's default --max-offset, sized
    # for NTP-disciplined clocks.
    HLC_MAX_CLOCK_OFFSET_MS: StrictInt = 500
    # AD-39 offset measurement: each node probes its tier peers' clocks this
    # often (the detection latency of a clock that runs away), and a
    # measurement counts toward fencing for this long -- three intervals,
    # so two lost probes in a row do not drop a peer's measurement.
    HLC_OFFSET_PROBE_INTERVAL_SECONDS: StrictFloat = 1.0
    HLC_OFFSET_SAMPLE_TTL_SECONDS: StrictFloat = 3.0

    # Cancelled Workflow Cleanup Settings (Section 6)
    CANCELLED_WORKFLOW_TTL: StrictFloat = (
        3600.0  # Seconds to retain cancelled workflow info (1 hour)
    )
    CANCELLED_WORKFLOW_CLEANUP_INTERVAL: StrictFloat = (
        60.0  # Seconds between cleanup checks
    )

    CANCELLED_WORKFLOW_TIMEOUT: StrictFloat = 60.0

    # Client Leadership Transfer Settings (Section 9)
    CLIENT_ORPHAN_GRACE_PERIOD: StrictFloat = (
        15.0  # Seconds to wait for leadership transfer cascade
    )
    CLIENT_ORPHAN_CHECK_INTERVAL: StrictFloat = (
        2.0  # Seconds between orphan grace period checks
    )
    CLIENT_RESPONSE_FRESHNESS_TIMEOUT: StrictFloat = (
        10.0  # Seconds to consider response stale after leadership change
    )
    # Seconds a client's job status query may take.
    CLIENT_STATUS_QUERY_TIMEOUT: StrictFloat = 5.0
    # Seconds one job submission attempt may take.
    CLIENT_SUBMISSION_TIMEOUT: StrictFloat = 10.0
    # Attempts a client makes to submit one job, and the leader redirects
    # it follows within one attempt.
    CLIENT_SUBMISSION_MAX_RETRIES: StrictInt = 5
    CLIENT_SUBMISSION_MAX_REDIRECTS: StrictInt = 3
    # Seconds a completed job waits for workflow results still in flight
    # (they travel apart from the terminal status, unordered with it):
    # one standard manager/gate push timeout, the longest a send already
    # under way can take to land.
    CLIENT_RESULT_DRAIN_TIMEOUT: StrictFloat = 5.0
    # Seconds a client keeps a finished job's status and results: as long
    # as a manager keeps a completed job (COMPLETED_JOB_MAX_AGE), past
    # which the cluster could not refresh them either.
    CLIENT_JOB_RETENTION_SECONDS: StrictFloat = 300.0

    # Manager Dead Node Cleanup Settings
    MANAGER_DEAD_WORKER_REAP_INTERVAL: StrictFloat = (
        900.0  # Seconds before reaping dead workers (15 minutes)
    )
    MANAGER_DEAD_PEER_REAP_INTERVAL: StrictFloat = (
        900.0  # Seconds before reaping dead manager peers (15 minutes)
    )
    MANAGER_DEAD_GATE_REAP_INTERVAL: StrictFloat = (
        900.0  # Seconds before reaping dead gates (15 minutes)
    )
    MANAGER_DEAD_NODE_CHECK_INTERVAL: StrictFloat = (
        60.0  # Seconds between dead node checks
    )
    MANAGER_RATE_LIMIT_CLEANUP_INTERVAL: StrictFloat = (
        60.0  # Seconds between rate limit client cleanup
    )

    # AD-30: Job Responsiveness Settings
    # Threshold for detecting stuck workflows - workers without progress for this duration are suspected
    JOB_RESPONSIVENESS_THRESHOLD: StrictFloat = (
        60.0  # Seconds without progress before suspicion
    )
    JOB_RESPONSIVENESS_CHECK_INTERVAL: StrictFloat = (
        15.0  # Seconds between responsiveness checks
    )

    # Manager Aggregate Health Alert Settings
    # Thresholds for triggering alerts when worker health degrades across the cluster
    MANAGER_HEALTH_ALERT_OVERLOADED_RATIO: StrictFloat = (
        0.5  # Alert when >= 50% of workers are overloaded
    )
    MANAGER_HEALTH_ALERT_NON_HEALTHY_RATIO: StrictFloat = (
        0.8  # Alert when >= 80% of workers are non-healthy (busy/stressed/overloaded)
    )
    # Seconds per sample of the dispatch throughput a manager reports in
    # its health heartbeat (AD-19).
    MANAGER_THROUGHPUT_INTERVAL_SECONDS: StrictFloat = 10.0

    # AD-34: Job Timeout Settings
    JOB_TIMEOUT_CHECK_INTERVAL: StrictFloat = 30.0  # Seconds between job timeout checks
    # AD-34 stuck detection: seconds a job with work in flight may go
    # without progress -- a workflow advancing its lifecycle or its
    # completed/failed counts, or an AD-26 extension -- before it counts as
    # stuck (AD-34's specified two minutes; the gate's all-DC threshold,
    # GATE_ALL_DC_STUCK_THRESHOLD, sits above it so a datacenter declares
    # first).
    JOB_STUCK_THRESHOLD: StrictFloat = 120.0

    # AD-44: Retry Budget Configuration
    RETRY_BUDGET_MAX: StrictInt = 50
    RETRY_BUDGET_PER_WORKFLOW_MAX: StrictInt = 5
    # A job's total workflow retries (re-dispatches after a failed dispatch
    # or a lost worker) when it names none. With the per-workflow cap of 3
    # retries (the SRE book's per-request limit is of that order: a request
    # that "has already failed three times" is not retried, "Handling
    # Overload"), a job of W workflows adds at most min(budget, 3W)
    # dispatches in a storm; the job budget binds from W >= 4. 10 is the
    # absolute floor established retry budgets keep for small callers
    # (Finagle RetryBudget: 20% of requests "on top of 10 retries per
    # second"; Envoy retry_budget: min_retry_concurrency 3) -- hyperscale
    # jobs are small callers (a handful of workflows). It lets a job of up
    # to 10 workflows survive the loss of a worker running all of them; 20
    # (the original design figure, given no rationale) doubles the
    # per-job storm bound and only changes outcomes for jobs of 4+
    # workflows losing workers 3 times, 6+ twice or 11+ once (sweep over
    # RetryBudgetManager, AD_44.md Part 6). A larger job names its own
    # ``retry_budget`` at submission (up to RETRY_BUDGET_MAX).
    RETRY_BUDGET_DEFAULT: StrictInt = 10
    RETRY_BUDGET_PER_WORKFLOW_DEFAULT: StrictInt = 3

    # AD-44: Best-Effort Configuration
    BEST_EFFORT_DEADLINE_MAX: StrictFloat = 3600.0
    BEST_EFFORT_DEADLINE_DEFAULT: StrictFloat = 300.0
    BEST_EFFORT_MIN_DCS_DEFAULT: StrictInt = 1
    BEST_EFFORT_DEADLINE_CHECK_INTERVAL: StrictFloat = 5.0
    # What a datacenter result that arrives after its best-effort job
    # completed does (AD-44 "Late DC Results"). "log": it is logged
    # (LateDatacenterResult) and not aggregated -- the job ends at once and
    # its unreported datacenters are cancelled. "update": a job completed
    # by reaching its min_dcs hands the client that result at once, lets
    # the unreported datacenters run on, folds each one's result into the
    # job result and pushes it again, and records the job's durable
    # terminal (AD-38) once every datacenter reported or the job's
    # best-effort deadline passed -- the bound the job itself declared.
    BEST_EFFORT_LATE_RESULT_POLICY: Literal["log", "update"] = "log"

    # AD-45: Adaptive Route Learning. The EWMA weight of each new observed
    # latency sample: a datacenter's time to accept a dispatch is a round
    # trip through its leader, smoothed as TCP smooths its round-trip time
    # (RFC 6298 section 2: alpha = 1/8) -- an estimate that follows a
    # lasting shift within a few dozen samples while one outlier moves it
    # an eighth of its excess.
    ADAPTIVE_ROUTING_ENABLED: StrictBool = True
    ADAPTIVE_ROUTING_EWMA_ALPHA: StrictFloat = 0.125
    ADAPTIVE_ROUTING_MIN_SAMPLES: StrictInt = 10
    ADAPTIVE_ROUTING_MAX_STALENESS_SECONDS: StrictFloat = 300.0
    ADAPTIVE_ROUTING_LATENCY_CAP_MS: StrictFloat = 60000.0

    # AD-42: SLO-aware routing and health -- latency targets and weights,
    # T-Digest compression and windows, health-classification ratios and
    # windows, AD-41 resource prediction, and gossip summary limits.
    SLO_TDIGEST_DELTA: StrictFloat = 100.0
    SLO_TDIGEST_MAX_UNMERGED: StrictInt = 2048
    SLO_WINDOW_DURATION_SECONDS: StrictFloat = 60.0
    SLO_MAX_WINDOWS: StrictInt = 5
    SLO_EVALUATION_WINDOW_SECONDS: StrictFloat = 300.0
    SLO_P50_TARGET_MS: StrictFloat = 50.0
    SLO_P95_TARGET_MS: StrictFloat = 200.0
    SLO_P99_TARGET_MS: StrictFloat = 500.0
    SLO_P50_WEIGHT: StrictFloat = 0.2
    SLO_P95_WEIGHT: StrictFloat = 0.5
    SLO_P99_WEIGHT: StrictFloat = 0.3
    SLO_MIN_SAMPLE_COUNT: StrictInt = 100
    SLO_FACTOR_MIN: StrictFloat = 0.5
    SLO_FACTOR_MAX: StrictFloat = 3.0
    SLO_SCORE_WEIGHT: StrictFloat = 0.4
    SLO_BUSY_P50_RATIO: StrictFloat = 1.5
    SLO_DEGRADED_P95_RATIO: StrictFloat = 2.0
    SLO_DEGRADED_P99_RATIO: StrictFloat = 3.0
    SLO_UNHEALTHY_P99_RATIO: StrictFloat = 5.0
    SLO_BUSY_WINDOW_SECONDS: StrictFloat = 60.0
    SLO_DEGRADED_WINDOW_SECONDS: StrictFloat = 180.0
    SLO_UNHEALTHY_WINDOW_SECONDS: StrictFloat = 300.0
    SLO_ENABLE_RESOURCE_PREDICTION: StrictBool = True
    SLO_CPU_LATENCY_CORRELATION: StrictFloat = 0.7
    SLO_MEMORY_LATENCY_CORRELATION: StrictFloat = 0.4
    SLO_PREDICTION_BLEND_WEIGHT: StrictFloat = 0.4
    SLO_GOSSIP_SUMMARY_TTL_SECONDS: StrictFloat = 30.0
    SLO_GOSSIP_MAX_JOBS_PER_HEARTBEAT: StrictInt = 100

    # Manager TCP Timeout Settings
    MANAGER_TCP_TIMEOUT_SHORT: StrictFloat = (
        2.0  # Short timeout for quick operations (peer sync, worker queries)
    )
    MANAGER_TCP_TIMEOUT_STANDARD: StrictFloat = (
        5.0  # Standard timeout for job dispatch, result forwarding
    )

    # Manager Batch Stats Settings
    # Seconds between batch stats pushes to clients (when no gates): the
    # gateless twin of GATE_BATCH_STATS_INTERVAL, derived there.
    MANAGER_BATCH_PUSH_INTERVAL: StrictFloat = 0.25

    # ==========================================================================
    # Gate Settings
    # ==========================================================================
    GATE_JOB_CLEANUP_INTERVAL: StrictFloat = 60.0  # Seconds between job cleanup checks
    GATE_RATE_LIMIT_CLEANUP_INTERVAL: StrictFloat = (
        60.0  # Seconds between rate limit client cleanup
    )
    # Seconds between AD-15 Tier-2 batch stats pushes to clients. The push is
    # a gate-fronted client's only live aggregate progress, so this is the
    # staleness bound on what the operator watches: Nielsen's response-time
    # limits keep continuous feedback under 1.0 s. The floor is the upstream
    # refresh (WORKER_PROGRESS_FLUSH_INTERVAL, 0.05 s): 0.25 s folds five
    # worker flushes into one push, four messages/s per job callback. Equal to
    # MANAGER_BATCH_PUSH_INTERVAL so gateless clients see the same cadence.
    GATE_BATCH_STATS_INTERVAL: StrictFloat = 0.25
    GATE_TCP_TIMEOUT_SHORT: StrictFloat = 2.0  # Short timeout for quick operations
    GATE_TCP_TIMEOUT_STANDARD: StrictFloat = (
        5.0  # Standard timeout for job dispatch, result forwarding
    )
    GATE_TCP_TIMEOUT_FORWARD: StrictFloat = 3.0  # Timeout for forwarding to peers
    # Seconds without a manager heartbeat before a gate counts a datacenter's
    # health as stale.
    # AD-52 section 8: a gate suspects a datacenter manager by a phi-accrual
    # detector over its heartbeats' arrival times, not a fixed staleness
    # window. Suspected at phi 12 -- Cassandra's guidance for cloud and
    # unstable links (8 is its LAN default; gate-to-datacenter edges cross
    # regions); intervals over a window of 1,000 and a deviation floor of
    # 100ms (Akka's and Cassandra's defaults); a 3s pause tolerated beyond
    # the expected interval (Akka cluster's acceptable-heartbeat-pause).
    PHI_ACCRUAL_THRESHOLD: StrictFloat = 12.0
    PHI_ACCRUAL_MAX_SAMPLE_SIZE: StrictInt = 1000
    PHI_ACCRUAL_MIN_STD_DEVIATION_SECONDS: StrictFloat = 0.1
    PHI_ACCRUAL_ACCEPTABLE_HEARTBEAT_PAUSE_SECONDS: StrictFloat = 3.0
    # Seconds per sample of the job-forwarding throughput a gate reports
    # in its health heartbeat (AD-19).
    GATE_THROUGHPUT_INTERVAL_SECONDS: StrictFloat = 10.0
    GATE_WORKFLOW_RESULT_TIMEOUT_SECONDS: StrictFloat = 300.0
    GATE_ALLOW_PARTIAL_WORKFLOW_RESULTS: StrictBool = False
    # Seconds one results reporter may take to connect and submit (and,
    # separately, to close) -- the gate's for a job's results, the
    # client's for each workflow's local files -- before it is abandoned
    # (logged) so a hung reporter neither leaks its submission nor holds
    # up the job. The longest per-operation deadline the reporter
    # backends set for themselves is 60 s (CloudwatchConfig.submit_timeout,
    # NewRelicConfig registration/shutdown_timeout, PrometheusConfig
    # auth_request_timeout); connect and submit are at most three such
    # operations (the client submits workflow and step results), so this
    # never cuts off a backend still within its own limits.
    REPORTER_SUBMISSION_TIMEOUT_SECONDS: StrictFloat = 180.0
    # Client update pushes a gate keeps per job for replay to reconnecting
    # clients.
    GATE_CLIENT_UPDATE_HISTORY_LIMIT: StrictInt = 200
    # AD-34 gate timeout tracking: seconds between checks, and seconds
    # without progress in every datacenter before a job counts as stuck.
    GATE_TIMEOUT_CHECK_INTERVAL: StrictFloat = 15.0
    GATE_ALL_DC_STUCK_THRESHOLD: StrictFloat = 180.0

    # Gate orphan job checks (Section 7). The grace before a lapsed-lease
    # job is taken over is derived from the gate tier's failure-detection
    # and election settings (gate/config.py derive_gate_orphan_grace_seconds).
    GATE_ORPHAN_CHECK_INTERVAL: StrictFloat = (
        2.0  # Seconds between orphan grace period checks
    )

    GATE_DEAD_PEER_REAP_INTERVAL: StrictFloat = 120.0
    GATE_DEAD_PEER_CHECK_INTERVAL: StrictFloat = 10.0
    GATE_QUORUM_STEPDOWN_CONSECUTIVE_FAILURES: StrictInt = 3

    # Gate SWIM hierarchical-detector bracket (AD-30).
    # Gates deliberately use a more conservative bracket than managers
    # (``SWIM_SUSPICION_*`` defaults to 1.5/8.0): a single gate
    # orchestrates hundreds-to-thousands of concurrent jobs, and a
    # false-positive gate death triggers leadership takeover for every
    # job it owned — fence-token bumps, hash-ring re-evaluation,
    # callback re-routing, and peer gates absorbing the load. The cost
    # of a false positive at gate fan-out dwarfs the cost of slower
    # true-positive detection. Tune with care.
    GATE_SWIM_GLOBAL_MIN_TIMEOUT: StrictFloat = 30.0
    GATE_SWIM_GLOBAL_MAX_TIMEOUT: StrictFloat = 120.0
    GATE_SWIM_JOB_MIN_TIMEOUT: StrictFloat = 5.0
    GATE_SWIM_JOB_MAX_TIMEOUT: StrictFloat = 30.0

    SPILLOVER_MAX_WAIT_SECONDS: StrictFloat = 60.0
    SPILLOVER_MAX_LATENCY_PENALTY_MS: StrictFloat = 100.0
    SPILLOVER_MIN_IMPROVEMENT_RATIO: StrictFloat = 0.5
    SPILLOVER_ENABLED: StrictBool = True
    CAPACITY_STALENESS_THRESHOLD_SECONDS: StrictFloat = 30.0

    # AD-36 routing load factor: 1 + utilization_weight * utilization +
    # queue_weight * queue + circuit_pressure_weight * circuit_pressure,
    # each signal in [0, 1]. The weights sum to 1 so a datacenter loaded
    # on every signal scores as if twice as far away -- the linear load
    # penalty of Envoy's least-request balancer at its default
    # active_request_bias of 1.0 (weight / (1 + load)). Load then decides
    # among datacenters at comparable distance, and AD-43 spillover, not
    # the score, moves work across regions when a datacenter cannot take
    # it. With no evidence that one signal predicts waiting better than
    # another, each weighs the same, as Kubernetes' NodeResourcesFit
    # scoring weighs each resource by default.
    ROUTING_UTILIZATION_WEIGHT: StrictFloat = 1.0 / 3.0
    ROUTING_QUEUE_WEIGHT: StrictFloat = 1.0 / 3.0
    ROUTING_CIRCUIT_PRESSURE_WEIGHT: StrictFloat = 1.0 / 3.0

    # ==========================================================================
    # Overload Detection Settings (AD-18)
    # ==========================================================================
    OVERLOAD_EMA_ALPHA: StrictFloat = (
        0.1  # Smoothing factor for baseline (lower = more stable)
    )
    # Seconds between a gate's or manager's own CPU/memory samples. The
    # detector's windows count samples, so this sets their time constants:
    # the current average spans OVERLOAD_CURRENT_WINDOW of these, the trend
    # OVERLOAD_TREND_WINDOW -- ten and twenty seconds at one second. A node
    # cannot change its overload verdict faster than this, so a request it
    # sheds is asked to retry after it.
    OVERLOAD_SAMPLE_INTERVAL_SECONDS: StrictFloat = 1.0
    OVERLOAD_CURRENT_WINDOW: StrictInt = 10  # Samples for current average
    OVERLOAD_TREND_WINDOW: StrictInt = 20  # Samples for trend calculation
    OVERLOAD_MIN_SAMPLES: StrictInt = 3  # Minimum samples before delta detection
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
    # Rate Limiting Settings (AD-24)
    # ==========================================================================
    RATE_LIMIT_CLIENT_IDLE_TIMEOUT: StrictFloat = (
        300.0  # Cleanup idle clients after 5min
    )
    # Per-client limits, each derived when unset (None) from the protocol
    # rate it bounds -- the derivations, with their arithmetic, are in
    # hyperscale/distributed/reliability/rate_limit_derivation.py and
    # docs/architecture/AD_24.md. Every count spans RATE_LIMIT_WINDOW_SECONDS
    # (derived: OVERLOAD_CURRENT_WINDOW x OVERLOAD_SAMPLE_INTERVAL_SECONDS)
    # and is twice the protocol's maximum per span (the sliding-window
    # counter's estimate bound).
    RATE_LIMIT_WINDOW_SECONDS: StrictFloat | None = None
    # MANAGER_HEARTBEAT_INTERVAL sends per span
    RATE_LIMIT_HEARTBEAT_MAX_REQUESTS: StrictInt | None = None
    # One per running workflow (<= worker cores) per WORKER_PROGRESS_FLUSH_INTERVAL
    RATE_LIMIT_PROGRESS_UPDATE_MAX_REQUESTS: StrictInt | None = None
    # Request-driven operations: the protocol bounds neither their rate nor
    # their concurrency, so unset they are unbounded (floods of them are
    # shed by the STRESSED budget and OVERLOADED shedding); set, a policy.
    RATE_LIMIT_STATS_UPDATE_MAX_REQUESTS: StrictInt | None = None
    RATE_LIMIT_JOB_SUBMIT_MAX_REQUESTS: StrictInt | None = None
    RATE_LIMIT_JOB_STATUS_MAX_REQUESTS: StrictInt | None = None
    RATE_LIMIT_WORKFLOW_DISPATCH_MAX_REQUESTS: StrictInt | None = None
    RATE_LIMIT_CANCEL_MAX_REQUESTS: StrictInt | None = None
    RATE_LIMIT_RECONNECT_MAX_REQUESTS: StrictInt | None = None
    RATE_LIMIT_DEFAULT_MAX_REQUESTS: StrictInt | None = None
    # A client's budget across all operations while the node is STRESSED:
    # its most active legitimate peer's AD-37-throttled traffic per span
    RATE_LIMIT_STRESSED_MAX_REQUESTS: StrictInt | None = None
    # Clients tracked before the least recently active is evicted: two per
    # connection the TCP server holds at once
    RATE_LIMIT_MAX_TRACKED_CLIENTS: StrictInt | None = None

    # ==========================================================================
    # Recovery and Thundering Herd Prevention Settings
    # ==========================================================================
    # Jitter settings - applied to recovery operations to prevent synchronized reconnection waves
    # Reduced from 0.1-2.0s to 0.05-0.5s for faster recovery while still preventing thundering herd
    RECOVERY_JITTER_MAX: StrictFloat = 0.5  # Reduced from 2.0 - faster recovery
    RECOVERY_JITTER_MIN: StrictFloat = 0.05  # Reduced from 0.1 - minimal delay

    # Concurrency caps - limit simultaneous recovery operations to prevent overload
    RECOVERY_MAX_CONCURRENT: StrictInt = (
        5  # Max concurrent recovery operations per node type
    )
    RECOVERY_SEMAPHORE_SIZE: StrictInt = (
        5  # Semaphore size for limiting concurrent recovery
    )
    DISPATCH_MAX_CONCURRENT_PER_WORKER: StrictInt = (
        3  # Max concurrent dispatches to a single worker
    )
    DISPATCH_MAX_CONCURRENT_WORKERS: StrictInt = (
        16  # Max workers to dispatch to concurrently for one workflow
    )
    DISPATCH_ROUTING_FAILURE_BASE_COOLDOWN: StrictFloat = (
        0.25  # Base seconds to cool down worker routing after TCP dispatch failure
    )
    DISPATCH_ROUTING_FAILURE_MAX_COOLDOWN: StrictFloat = (
        5.0  # Max seconds to cool down worker routing after repeated failures
    )
    DISPATCH_ROUTING_READINESS_COOLDOWN: StrictFloat = (
        0.5  # Seconds to cool down routing after worker-side readiness rejection
    )

    # Message queue backpressure - prevent memory exhaustion under load
    MESSAGE_QUEUE_MAX_SIZE: StrictInt = (
        1000  # Max pending messages per client connection
    )
    MESSAGE_QUEUE_WARN_SIZE: StrictInt = 800  # Warn threshold (80% of max)

    # ==========================================================================
    # Healthcheck Extension Settings (AD-26)
    # ==========================================================================
    EXTENSION_BASE_DEADLINE: StrictFloat = 30.0  # Base deadline in seconds
    EXTENSION_MIN_GRANT: StrictFloat = 1.0  # Minimum extension grant in seconds
    EXTENSION_MAX_EXTENSIONS: StrictInt = 5  # Maximum extensions per cycle
    EXTENSION_EVICTION_THRESHOLD: StrictInt = 3  # Failures before eviction
    EXTENSION_EXHAUSTION_WARNING_THRESHOLD: StrictInt = (
        1  # Remaining extensions to trigger warning
    )
    EXTENSION_EXHAUSTION_GRACE_PERIOD: StrictFloat = (
        10.0  # Seconds of grace after exhaustion before kill
    )
    # Phase H2 — default per-workflow timeout multiplier when neither
    # JobSubmission.timeout_seconds (set explicitly by the client) nor
    # Workflow.timeout (overridden in the workflow class) is provided.
    # The deadline is then ``workflow.duration × multiplier``. Default
    # 1.5 per the AD-26/AD-34 design conversation: workflows of declared
    # duration D get a buffered deadline of 1.5×D before SUSPECT.
    HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER: StrictFloat = 1.5
    # Phase H6 — cluster-wide tolerated false-positive rate for the
    # AD-26 throughput witness. Default 0.01 (1%) per the production-
    # fleet rationale: at ~24K workflows/day and AD-26 max_extensions
    # =5, this gives ~240 false denies/day across the fleet, an
    # acceptable rate when extensions are bounded. The hierarchical
    # α-budget allocator splits this across DC → manager → worker →
    # workflow via Benjamini-Hochberg FDR.
    HYPERSCALE_EXTENSION_FPR_BUDGET: StrictFloat = 0.01
    # Phase H4 — worker autonomous extension trigger. The trigger
    # background loop checks active workflows at this cadence; for
    # each workflow whose elapsed time exceeds deadline ×
    # ``HYPERSCALE_EXTENSION_LOOKAHEAD_FRACTION`` AND has shown
    # forward progress since the last extension request, it
    # invokes ``WorkerServer.request_extension`` with a complete
    # ``WorkflowProgressSnapshot``. Defaults aligned with the
    # heartbeat cadence so requests piggyback on the next outbound
    # heartbeat without lag.
    HYPERSCALE_EXTENSION_TRIGGER_INTERVAL: StrictStr = "5s"
    # Fraction of the workflow's deadline at which the autonomous
    # trigger starts requesting extensions. 0.75 = "request when
    # 75% of the budget is gone." Picked so a typical 60s workflow
    # asks for its first extension at the 45s mark — comfortably
    # before the deadline fires but late enough that short
    # workflows complete normally without ever requesting.
    HYPERSCALE_EXTENSION_LOOKAHEAD_FRACTION: StrictFloat = 0.75

    # ==========================================================================
    # Orphaned Workflow Scanner Settings
    # ==========================================================================
    ORPHAN_SCAN_INTERVAL: StrictFloat = (
        120.0  # Seconds between orphan scans (2 minutes)
    )
    ORPHAN_SCAN_WORKER_TIMEOUT: StrictFloat = (
        5.0  # Timeout for querying workers during scan
    )

    # ==========================================================================
    # Time-Windowed Stats Streaming Settings
    # ==========================================================================
    STATS_WINDOW_SIZE_MS: StrictFloat = (
        50.0  # Window bucket size in milliseconds (smaller = more granular)
    )
    # Drift tolerance allows for network latency between worker send and manager receive
    # Workers now send directly (not buffered), so we only need network latency margin
    STATS_DRIFT_TOLERANCE_MS: StrictFloat = 25.0  # Network latency allowance only
    STATS_PUSH_INTERVAL_MS: StrictFloat = (
        50.0  # How often to flush windows and push (ms)
    )
    STATS_MAX_WINDOW_AGE_MS: StrictFloat = (
        5000.0  # Max age before window is dropped (cleanup)
    )

    # Status update processing interval (seconds) - controls how often _process_status_updates runs
    # during workflow completion wait. Lower values = more responsive UI updates.
    STATUS_UPDATE_POLL_INTERVAL: StrictFloat = 0.05  # 50ms default for real-time UI

    # ==========================================================================
    # Manager Stats Buffer Settings (AD-23)
    # ==========================================================================
    # Tiered retention for stats with backpressure based on buffer fill levels
    MANAGER_STATS_HOT_MAX_ENTRIES: StrictInt = (
        1000  # Max entries in hot tier ring buffer
    )
    MANAGER_STATS_THROTTLE_THRESHOLD: StrictFloat = 0.70  # Throttle at 70% fill
    MANAGER_STATS_BATCH_THRESHOLD: StrictFloat = 0.85  # Batch-only at 85% fill
    MANAGER_STATS_REJECT_THRESHOLD: StrictFloat = (
        0.95  # Reject non-critical at 95% fill
    )
    MANAGER_STATS_BUFFER_HIGH_WATERMARK: StrictInt = 1000  # THROTTLE trigger
    MANAGER_STATS_BUFFER_CRITICAL_WATERMARK: StrictInt = 5000  # BATCH trigger
    MANAGER_STATS_BUFFER_REJECT_WATERMARK: StrictInt = 10000  # REJECT trigger
    MANAGER_PROGRESS_NORMAL_RATIO: StrictFloat = 0.8  # >= 80% throughput = NORMAL
    MANAGER_PROGRESS_SLOW_RATIO: StrictFloat = 0.5  # >= 50% throughput = SLOW
    MANAGER_PROGRESS_DEGRADED_RATIO: StrictFloat = 0.2  # >= 20% throughput = DEGRADED

    # ==========================================================================
    # Cross-DC Correlation Settings (Phase 7)
    # ==========================================================================
    # These settings control correlation detection for cascade eviction prevention
    # Tuned for globally distributed datacenters with high latency
    CROSS_DC_CORRELATION_WINDOW: StrictFloat = (
        30.0  # Seconds window for correlation detection
    )
    CROSS_DC_CORRELATION_LOW_THRESHOLD: StrictInt = (
        2  # Min DCs failing for LOW correlation
    )
    CROSS_DC_CORRELATION_MEDIUM_THRESHOLD: StrictInt = (
        3  # Min DCs failing for MEDIUM correlation
    )
    CROSS_DC_CORRELATION_HIGH_COUNT_THRESHOLD: StrictInt = (
        4  # Min DCs failing for HIGH (count)
    )
    CROSS_DC_CORRELATION_HIGH_FRACTION: StrictFloat = (
        0.5  # Fraction of DCs for HIGH (requires count too)
    )
    CROSS_DC_CORRELATION_BACKOFF: StrictFloat = (
        60.0  # Backoff duration after correlation detected
    )

    # Anti-flapping settings for cross-DC correlation
    CROSS_DC_FAILURE_CONFIRMATION: StrictFloat = (
        5.0  # Seconds failure must persist before counting
    )
    CROSS_DC_RECOVERY_CONFIRMATION: StrictFloat = (
        30.0  # Seconds recovery must persist before healthy
    )
    CROSS_DC_FLAP_THRESHOLD: StrictInt = (
        3  # State changes in window to be considered flapping
    )
    CROSS_DC_FLAP_DETECTION_WINDOW: StrictFloat = 120.0  # Window for flap detection
    CROSS_DC_FLAP_COOLDOWN: StrictFloat = (
        300.0  # Cooldown after flapping before can be stable
    )

    # Latency-based correlation settings
    CROSS_DC_ENABLE_LATENCY_CORRELATION: StrictBool = True
    CROSS_DC_LATENCY_ELEVATED_THRESHOLD_MS: StrictFloat = (
        100.0  # Latency above this is elevated
    )
    CROSS_DC_LATENCY_CRITICAL_THRESHOLD_MS: StrictFloat = (
        500.0  # Latency above this is critical
    )
    CROSS_DC_MIN_LATENCY_SAMPLES: StrictInt = 3  # Min samples before latency decisions
    CROSS_DC_LATENCY_SAMPLE_WINDOW: StrictFloat = 60.0  # Window for latency samples
    CROSS_DC_LATENCY_CORRELATION_FRACTION: StrictFloat = (
        0.5  # Fraction of DCs for latency correlation
    )

    # Extension-based correlation settings
    CROSS_DC_ENABLE_EXTENSION_CORRELATION: StrictBool = True
    CROSS_DC_EXTENSION_COUNT_THRESHOLD: StrictInt = (
        2  # Extensions to consider DC under load
    )
    CROSS_DC_EXTENSION_CORRELATION_FRACTION: StrictFloat = (
        0.5  # Fraction of DCs for extension correlation
    )
    CROSS_DC_EXTENSION_WINDOW: StrictFloat = 120.0  # Window for extension tracking

    # LHM-based correlation settings
    CROSS_DC_ENABLE_LHM_CORRELATION: StrictBool = True
    CROSS_DC_LHM_STRESSED_THRESHOLD: StrictInt = (
        3  # LHM score (0-8) to consider DC stressed
    )
    CROSS_DC_LHM_CORRELATION_FRACTION: StrictFloat = (
        0.5  # Fraction of DCs for LHM correlation
    )

    # ==========================================================================
    # Discovery Service Settings (AD-28)
    # ==========================================================================
    # Cluster and Environment Isolation (AD-28 Issue 2)
    CLUSTER_ID: StrictStr = "hyperscale"  # Cluster identifier for isolation
    ENVIRONMENT_ID: StrictStr = "default"  # Environment identifier for isolation

    # DNS-based peer discovery
    DISCOVERY_DNS_NAMES: StrictStr = (
        ""  # Comma-separated DNS names for manager discovery
    )
    DISCOVERY_DNS_CACHE_TTL: StrictFloat = 60.0  # DNS cache TTL in seconds
    DISCOVERY_DNS_TIMEOUT: StrictFloat = 5.0  # DNS resolution timeout in seconds
    DISCOVERY_DEFAULT_PORT: StrictInt = 9091  # Default port for discovered peers

    # DNS Security (Phase 2) - Protects against cache poisoning, hijacking, spoofing
    DISCOVERY_DNS_ALLOWED_CIDRS: StrictStr = (
        ""  # Comma-separated CIDRs (e.g., "10.0.0.0/8,172.16.0.0/12")
    )
    DISCOVERY_DNS_BLOCK_PRIVATE_FOR_PUBLIC: StrictBool = (
        False  # Block private IPs for public hostnames
    )
    DISCOVERY_DNS_DETECT_IP_CHANGES: StrictBool = (
        True  # Enable IP change anomaly detection
    )
    DISCOVERY_DNS_MAX_IP_CHANGES: StrictInt = (
        5  # Max IP changes before rapid rotation alert
    )
    DISCOVERY_DNS_IP_CHANGE_WINDOW: StrictFloat = (
        300.0  # Window for tracking IP changes (5 min)
    )
    DISCOVERY_DNS_REJECT_ON_VIOLATION: StrictBool = (
        True  # Reject IPs failing security validation
    )

    # Locality configuration
    DISCOVERY_DATACENTER_ID: StrictStr = (
        ""  # Local datacenter ID for locality-aware selection
    )
    DISCOVERY_REGION_ID: StrictStr = ""  # Local region ID for locality-aware selection
    DISCOVERY_PREFER_SAME_DC: StrictBool = True  # Prefer same-DC peers over cross-DC

    # Adaptive peer selection (Power of Two Choices with EWMA)
    DISCOVERY_CANDIDATE_SET_SIZE: StrictInt = (
        3  # Number of candidates for power-of-two selection
    )
    DISCOVERY_EWMA_ALPHA: StrictFloat = (
        0.3  # EWMA smoothing factor for latency tracking
    )
    DISCOVERY_BASELINE_LATENCY_MS: StrictFloat = (
        50.0  # Baseline latency for EWMA initialization
    )
    DISCOVERY_LATENCY_MULTIPLIER_THRESHOLD: StrictFloat = (
        2.0  # Latency threshold multiplier
    )
    DISCOVERY_MIN_PEERS_PER_TIER: StrictInt = 1  # Minimum peers per locality tier

    # Probing and health
    DISCOVERY_MAX_CONCURRENT_DNS_RESOLUTIONS: StrictInt = (
        10  # Most DNS resolutions in flight at once
    )
    DISCOVERY_PROBE_INTERVAL: StrictFloat = 30.0  # Seconds between peer health probes
    DISCOVERY_FAILURE_DECAY_INTERVAL: StrictFloat = (
        60.0  # Seconds between failure count decay
    )

    # ==========================================================================
    # Bounded Pending Response Queues Settings (AD-32)
    # ==========================================================================
    # Priority-aware bounded execution with load shedding
    # CRITICAL defaults to unshed; SWIM uses its own bounded reserve.
    PENDING_RESPONSE_MAX_CONCURRENT: StrictInt = (
        1000  # Global limit across all priorities
    )
    PENDING_RESPONSE_SWIM_LIMIT: StrictInt = (
        1000  # Dedicated SWIM/control in-flight cap independent of NORMAL
    )
    PENDING_RESPONSE_HIGH_LIMIT: StrictInt = 500  # HIGH priority limit
    PENDING_RESPONSE_NORMAL_LIMIT: StrictInt = 300  # NORMAL priority limit
    PENDING_RESPONSE_LOW_LIMIT: StrictInt = 200  # LOW priority limit (shed first)
    PENDING_RESPONSE_WARN_THRESHOLD: StrictFloat = (
        0.8  # Log warning at this % of global limit
    )

    # Client-side per-destination queue settings (AD-32)
    OUTGOING_QUEUE_SIZE: StrictInt = 500  # Per-destination queue size
    OUTGOING_OVERFLOW_SIZE: StrictInt = 100  # Overflow ring buffer size
    OUTGOING_MAX_DESTINATIONS: StrictInt = (
        1000  # Max tracked destinations (LRU evicted)
    )

    MTLS_STRICT_MODE: StrictStr = "false"

    @classmethod
    def types_map(cls) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            "MTLS_STRICT_MODE": str,
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
            "MERCURY_SYNC_TASK_RUNNER_KEEP": int,
            "MERCURY_SYNC_MAX_REQUEST_CACHE_SIZE": int,
            "MERCURY_SYNC_ENABLE_REQUEST_CACHING": parse_bool_envar,
            "MERCURY_SYNC_UDP_SERVER_RCVBUF": int,
            # Monitor settings
            "MERCURY_SYNC_MONITOR_SAMPLE_WINDOW": str,
            "MERCURY_SYNC_MONITOR_SAMPLE_INTERVAL": str,
            "MERCURY_SYNC_PROCESS_JOB_CPU_LIMIT": float,
            "MERCURY_SYNC_PROCESS_JOB_MEMORY_LIMIT": int,
            # SWIM settings
            "SWIM_MAX_PROBE_TIMEOUT": int,
            "SWIM_MIN_PROBE_TIMEOUT": int,
            "SWIM_CURRENT_TIMEOUT": int,
            "SWIM_UDP_POLL_INTERVAL": int,
            "SWIM_SUSPICION_MIN_TIMEOUT": float,
            "SWIM_SUSPICION_MAX_TIMEOUT": float,
            "SWIM_NO_WITNESS_SUSPICION_TIMEOUT": float,
            "BURST_FAILURE_THRESHOLD": int,
            "BURST_FAILURE_WINDOW_SECONDS": float,
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
            "CLUSTER_FORMATION_INTERVAL_SECONDS": float,
            "CLUSTER_TOMBSTONE_RETENTION_SECONDS": float,
            "CLUSTER_WATCH_WAIT_SECONDS": float,
            "CLUSTER_SNAPSHOT_ENTRIES": int,
            "CLUSTER_SNAPSHOT_CATCH_UP_ENTRIES": int,
            "RAFT_SET_ASIDE_RETAINED": int,
            "RAFT_LEADER_LEASES_ENABLED": parse_bool_envar,
            "RAFT_CLOCK_DRIFT_BOUND": float,
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
            # Worker cluster-connection liveness settings
            "WORKER_CLUSTER_LIVENESS_CHECK_INTERVAL": float,
            "WORKER_CLUSTER_HEARTBEAT_STALENESS_THRESHOLD": float,
            "WORKER_CLUSTER_REJOIN_BASE_BACKOFF": float,
            # Worker load-sampling settings
            "WORKER_OVERLOAD_POLL_INTERVAL": float,
            "WORKER_THROUGHPUT_INTERVAL_SECONDS": float,
            # Worker cancellation polling settings
            "WORKER_CANCELLATION_POLL_INTERVAL": float,
            # Worker backpressure delay settings (AD-37)
            "WORKER_BACKPRESSURE_THROTTLE_DELAY_MS": int,
            "WORKER_BACKPRESSURE_BATCH_DELAY_MS": int,
            "WORKER_BACKPRESSURE_REJECT_DELAY_MS": int,
            # Worker TCP timeout settings
            "WORKER_TCP_TIMEOUT_SHORT": float,
            "WORKER_TCP_TIMEOUT_STANDARD": float,
            "WORKER_PROGRESS_SEND_TIMEOUT": float,
            "WORKER_HEARTBEAT_SEND_TIMEOUT": float,
            "WORKER_EXECUTION_UPDATE_WAIT": float,
            "WORKER_WORKFLOW_CANCEL_TIMEOUT": float,
            "WORKER_PENDING_RESULT_LIMIT": int,
            "WORKER_RESULT_MAX_RETRIES": int,
            "WORKER_RESULT_RETRY_BASE_DELAY": float,
            "WORKER_COMPLETION_TIMES_MAX_SAMPLES": int,
            # Worker process pool startup budget
            "WORKER_POOL_STARTUP_TIMEOUT_SECONDS": float,
            # Worker orphan grace period settings
            "WORKER_ORPHAN_CHECK_INTERVAL": float,
            # Worker job leadership transfer settings (Section 8)
            "WORKER_PENDING_TRANSFER_TTL": float,
            # Manager startup and dispatch settings
            "MANAGER_STARTUP_SYNC_DELAY": float,
            "MANAGER_STATE_SYNC_TIMEOUT": float,
            "MANAGER_STATE_SYNC_RETRIES": int,
            "MANAGER_DISPATCH_CORE_WAIT_TIMEOUT": float,
            "MANAGER_HEARTBEAT_INTERVAL": float,
            "MAX_WORKERS_PER_MANAGER": int,
            "JOB_CONCURRENCY_CAP_PER_DC": int,
            "JOB_CLASS_CONCURRENCY_CAPS": str,
            "MANAGER_PEER_SYNC_INTERVAL": float,
            "MANAGER_PEER_JOB_SYNC_INTERVAL": float,
            # Job cleanup settings
            "COMPLETED_JOB_MAX_AGE": float,
            "FAILED_JOB_MAX_AGE": float,
            "JOB_CLEANUP_INTERVAL": float,
            "RESOURCE_GUARD_ENABLED": parse_bool_envar,
            "RESOURCE_GUARD_MAX_CPU_PERCENT": float,
            "RESOURCE_GUARD_MAX_MEMORY_BYTES": int,
            "RESOURCE_GUARD_WARNING_THRESHOLD": float,
            "RESOURCE_GUARD_THROTTLE_THRESHOLD": float,
            "RESOURCE_GUARD_KILL_THRESHOLD": float,
            "RESOURCE_GUARD_WARNING_GRACE_SECONDS": float,
            "RESOURCE_GUARD_KILL_GRACE_SECONDS": float,
            "RESOURCE_VIEW_STALENESS_SECONDS": float,
            "HLC_MAX_CLOCK_OFFSET_MS": int,
            "HLC_OFFSET_PROBE_INTERVAL_SECONDS": float,
            "HLC_OFFSET_SAMPLE_TTL_SECONDS": float,
            # Cancelled workflow cleanup settings (Section 6)
            "CANCELLED_WORKFLOW_TTL": float,
            "CANCELLED_WORKFLOW_CLEANUP_INTERVAL": float,
            # Client leadership transfer settings (Section 9)
            "CLIENT_ORPHAN_GRACE_PERIOD": float,
            "CLIENT_ORPHAN_CHECK_INTERVAL": float,
            "CLIENT_RESPONSE_FRESHNESS_TIMEOUT": float,
            "CLIENT_STATUS_QUERY_TIMEOUT": float,
            "CLIENT_SUBMISSION_TIMEOUT": float,
            "CLIENT_SUBMISSION_MAX_RETRIES": int,
            "CLIENT_SUBMISSION_MAX_REDIRECTS": int,
            "CLIENT_RESULT_DRAIN_TIMEOUT": float,
            "CLIENT_JOB_RETENTION_SECONDS": float,
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
            # Manager health alert settings
            "MANAGER_HEALTH_ALERT_OVERLOADED_RATIO": float,
            "MANAGER_HEALTH_ALERT_NON_HEALTHY_RATIO": float,
            "MANAGER_THROUGHPUT_INTERVAL_SECONDS": float,
            # AD-44 retry budget settings
            "RETRY_BUDGET_MAX": int,
            "RETRY_BUDGET_PER_WORKFLOW_MAX": int,
            "RETRY_BUDGET_DEFAULT": int,
            "RETRY_BUDGET_PER_WORKFLOW_DEFAULT": int,
            # AD-44 best-effort settings
            "BEST_EFFORT_DEADLINE_MAX": float,
            "BEST_EFFORT_DEADLINE_DEFAULT": float,
            "BEST_EFFORT_MIN_DCS_DEFAULT": int,
            "BEST_EFFORT_DEADLINE_CHECK_INTERVAL": float,
            "BEST_EFFORT_LATE_RESULT_POLICY": str,
            # Gate settings
            "GATE_JOB_CLEANUP_INTERVAL": float,
            "GATE_RATE_LIMIT_CLEANUP_INTERVAL": float,
            "GATE_BATCH_STATS_INTERVAL": float,
            "GATE_TCP_TIMEOUT_SHORT": float,
            "GATE_TCP_TIMEOUT_STANDARD": float,
            "GATE_TCP_TIMEOUT_FORWARD": float,
            "PHI_ACCRUAL_THRESHOLD": float,
            "PHI_ACCRUAL_MAX_SAMPLE_SIZE": int,
            "PHI_ACCRUAL_MIN_STD_DEVIATION_SECONDS": float,
            "PHI_ACCRUAL_ACCEPTABLE_HEARTBEAT_PAUSE_SECONDS": float,
            "GATE_THROUGHPUT_INTERVAL_SECONDS": float,
            "GATE_CLIENT_UPDATE_HISTORY_LIMIT": int,
            "GATE_TIMEOUT_CHECK_INTERVAL": float,
            "GATE_ALL_DC_STUCK_THRESHOLD": float,
            "GATE_WORKFLOW_RESULT_TIMEOUT_SECONDS": float,
            "GATE_ALLOW_PARTIAL_WORKFLOW_RESULTS": parse_bool_envar,
            "REPORTER_SUBMISSION_TIMEOUT_SECONDS": float,
            # Gate orphan grace period settings (Section 7)
            "GATE_ORPHAN_CHECK_INTERVAL": float,
            "GATE_DEAD_PEER_REAP_INTERVAL": float,
            "GATE_DEAD_PEER_CHECK_INTERVAL": float,
            "GATE_QUORUM_STEPDOWN_CONSECUTIVE_FAILURES": int,
            # Gate SWIM hierarchical-detector bracket (AD-30)
            "GATE_SWIM_GLOBAL_MIN_TIMEOUT": float,
            "GATE_SWIM_GLOBAL_MAX_TIMEOUT": float,
            "MANAGER_SWIM_JOB_MIN_TIMEOUT": float,
            "MANAGER_SWIM_JOB_MAX_TIMEOUT": float,
            "GATE_SWIM_JOB_MIN_TIMEOUT": float,
            "GATE_SWIM_JOB_MAX_TIMEOUT": float,
            # Overload detection settings (AD-18)
            "OVERLOAD_EMA_ALPHA": float,
            "OVERLOAD_SAMPLE_INTERVAL_SECONDS": float,
            "OVERLOAD_CURRENT_WINDOW": int,
            "OVERLOAD_TREND_WINDOW": int,
            "OVERLOAD_MIN_SAMPLES": int,
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
            # Rate limiting settings (AD-24)
            "RATE_LIMIT_CLIENT_IDLE_TIMEOUT": float,
            "RATE_LIMIT_WINDOW_SECONDS": float,
            "RATE_LIMIT_HEARTBEAT_MAX_REQUESTS": int,
            "RATE_LIMIT_PROGRESS_UPDATE_MAX_REQUESTS": int,
            "RATE_LIMIT_STATS_UPDATE_MAX_REQUESTS": int,
            "RATE_LIMIT_JOB_SUBMIT_MAX_REQUESTS": int,
            "RATE_LIMIT_JOB_STATUS_MAX_REQUESTS": int,
            "RATE_LIMIT_WORKFLOW_DISPATCH_MAX_REQUESTS": int,
            "RATE_LIMIT_CANCEL_MAX_REQUESTS": int,
            "RATE_LIMIT_RECONNECT_MAX_REQUESTS": int,
            "RATE_LIMIT_DEFAULT_MAX_REQUESTS": int,
            "RATE_LIMIT_STRESSED_MAX_REQUESTS": int,
            "RATE_LIMIT_MAX_TRACKED_CLIENTS": int,
            # Healthcheck extension settings (AD-26)
            "EXTENSION_BASE_DEADLINE": float,
            "EXTENSION_MIN_GRANT": float,
            "EXTENSION_MAX_EXTENSIONS": int,
            "EXTENSION_EVICTION_THRESHOLD": int,
            "HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER": float,
            "HYPERSCALE_EXTENSION_FPR_BUDGET": float,
            "HYPERSCALE_EXTENSION_TRIGGER_INTERVAL": str,
            "HYPERSCALE_EXTENSION_LOOKAHEAD_FRACTION": float,
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
            # Manager stats buffer settings (AD-23)
            "MANAGER_STATS_HOT_MAX_ENTRIES": int,
            "MANAGER_STATS_THROTTLE_THRESHOLD": float,
            "MANAGER_STATS_BATCH_THRESHOLD": float,
            "MANAGER_STATS_REJECT_THRESHOLD": float,
            "MANAGER_STATS_BUFFER_HIGH_WATERMARK": int,
            "MANAGER_STATS_BUFFER_CRITICAL_WATERMARK": int,
            "MANAGER_STATS_BUFFER_REJECT_WATERMARK": int,
            "MANAGER_PROGRESS_NORMAL_RATIO": float,
            "MANAGER_PROGRESS_SLOW_RATIO": float,
            "MANAGER_PROGRESS_DEGRADED_RATIO": float,
            # Cluster and environment isolation (AD-28 Issue 2)
            "CLUSTER_ID": str,
            "ENVIRONMENT_ID": str,
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
            "CROSS_DC_ENABLE_LATENCY_CORRELATION": parse_bool_envar,
            "CROSS_DC_LATENCY_ELEVATED_THRESHOLD_MS": float,
            "CROSS_DC_LATENCY_CRITICAL_THRESHOLD_MS": float,
            "CROSS_DC_MIN_LATENCY_SAMPLES": int,
            "CROSS_DC_LATENCY_SAMPLE_WINDOW": float,
            "CROSS_DC_LATENCY_CORRELATION_FRACTION": float,
            # Extension-based correlation settings
            "CROSS_DC_ENABLE_EXTENSION_CORRELATION": parse_bool_envar,
            "CROSS_DC_EXTENSION_COUNT_THRESHOLD": int,
            "CROSS_DC_EXTENSION_CORRELATION_FRACTION": float,
            "CROSS_DC_EXTENSION_WINDOW": float,
            # LHM-based correlation settings
            "CROSS_DC_ENABLE_LHM_CORRELATION": parse_bool_envar,
            "CROSS_DC_LHM_STRESSED_THRESHOLD": int,
            "CROSS_DC_LHM_CORRELATION_FRACTION": float,
            # Recovery and thundering herd settings
            "RECOVERY_JITTER_MAX": float,
            "RECOVERY_JITTER_MIN": float,
            "RECOVERY_MAX_CONCURRENT": int,
            "RECOVERY_SEMAPHORE_SIZE": int,
            "DISPATCH_MAX_CONCURRENT_PER_WORKER": int,
            "DISPATCH_MAX_CONCURRENT_WORKERS": int,
            "DISPATCH_ROUTING_FAILURE_BASE_COOLDOWN": float,
            "DISPATCH_ROUTING_FAILURE_MAX_COOLDOWN": float,
            "DISPATCH_ROUTING_READINESS_COOLDOWN": float,
            "MESSAGE_QUEUE_MAX_SIZE": int,
            "MESSAGE_QUEUE_WARN_SIZE": int,
            # Bounded pending response queues settings (AD-32)
            "PENDING_RESPONSE_MAX_CONCURRENT": int,
            "PENDING_RESPONSE_SWIM_LIMIT": int,
            "PENDING_RESPONSE_HIGH_LIMIT": int,
            "PENDING_RESPONSE_NORMAL_LIMIT": int,
            "PENDING_RESPONSE_LOW_LIMIT": int,
            "PENDING_RESPONSE_WARN_THRESHOLD": float,
            # Client-side queue settings (AD-32)
            "OUTGOING_QUEUE_SIZE": int,
            "OUTGOING_OVERFLOW_SIZE": int,
            "OUTGOING_MAX_DESTINATIONS": int,
            "CANCELLED_WORKFLOW_TIMEOUT": float,
            # Transport / TLS settings
            "MERCURY_SYNC_AUTH_SECRET_PREVIOUS": str,
            "MERCURY_SYNC_TCP_SERVER_BACKLOG": int,
            "MERCURY_SYNC_MAX_ACCEPTED_TCP_CONNECTIONS": int,
            "MERCURY_SYNC_VERIFY_SSL_CERT": str,
            "MERCURY_SYNC_TLS_VERIFY_HOSTNAME": str,
            "MERCURY_SYNC_HOST_RESOLUTION_TTL": float,
            "MERCURY_SYNC_HOST_RESOLUTION_TIMEOUT": float,
            "MERCURY_SYNC_CONNECT_TIMEOUT": str,
            "MERCURY_SYNC_RETRY_INTERVAL": str,
            "MERCURY_SYNC_SEND_RETRIES": int,
            "MERCURY_SYNC_CONNECT_RETRIES": int,
            "MERCURY_SYNC_MAX_RUNNING_WORKFLOWS": int,
            "MERCURY_SYNC_MAX_PENDING_WORKFLOWS": int,
            "MERCURY_SYNC_CONTEXT_POLL_RATE": str,
            "MERCURY_SYNC_SHUTDOWN_POLL_RATE": str,
            "MERCURY_SYNC_DUPLICATE_JOB_POLICY": str,
            # Job lease settings
            "JOB_LEASE_DURATION": float,
            "JOB_LEASE_CLEANUP_INTERVAL": float,
            # Idempotency settings (AD-40)
            "IDEMPOTENCY_PENDING_TTL_SECONDS": float,
            "IDEMPOTENCY_COMMITTED_TTL_SECONDS": float,
            "IDEMPOTENCY_REJECTED_TTL_SECONDS": float,
            "IDEMPOTENCY_MAX_ENTRIES": int,
            "IDEMPOTENCY_CLEANUP_INTERVAL_SECONDS": float,
            "IDEMPOTENCY_WAIT_FOR_PENDING": parse_bool_envar,
            "IDEMPOTENCY_PENDING_WAIT_TIMEOUT": float,
            # Worker registration settings
            "WORKER_REGISTRATION_MAX_RETRIES": int,
            "WORKER_REGISTRATION_BASE_DELAY": float,
            "WORKER_INITIAL_REGISTRATION_JITTER_MAX": float,
            # Job responsiveness / timeout settings
            "JOB_RESPONSIVENESS_THRESHOLD": float,
            "JOB_RESPONSIVENESS_CHECK_INTERVAL": float,
            "JOB_TIMEOUT_CHECK_INTERVAL": float,
            "JOB_STUCK_THRESHOLD": float,
            # Adaptive routing settings
            "ADAPTIVE_ROUTING_ENABLED": parse_bool_envar,
            "ADAPTIVE_ROUTING_EWMA_ALPHA": float,
            "ADAPTIVE_ROUTING_MIN_SAMPLES": int,
            "ADAPTIVE_ROUTING_MAX_STALENESS_SECONDS": float,
            "ADAPTIVE_ROUTING_LATENCY_CAP_MS": float,
            "SLO_TDIGEST_DELTA": float,
            "SLO_TDIGEST_MAX_UNMERGED": int,
            "SLO_WINDOW_DURATION_SECONDS": float,
            "SLO_MAX_WINDOWS": int,
            "SLO_EVALUATION_WINDOW_SECONDS": float,
            "SLO_P50_TARGET_MS": float,
            "SLO_P95_TARGET_MS": float,
            "SLO_P99_TARGET_MS": float,
            "SLO_P50_WEIGHT": float,
            "SLO_P95_WEIGHT": float,
            "SLO_P99_WEIGHT": float,
            "SLO_MIN_SAMPLE_COUNT": int,
            "SLO_FACTOR_MIN": float,
            "SLO_FACTOR_MAX": float,
            "SLO_SCORE_WEIGHT": float,
            "SLO_BUSY_P50_RATIO": float,
            "SLO_DEGRADED_P95_RATIO": float,
            "SLO_DEGRADED_P99_RATIO": float,
            "SLO_UNHEALTHY_P99_RATIO": float,
            "SLO_BUSY_WINDOW_SECONDS": float,
            "SLO_DEGRADED_WINDOW_SECONDS": float,
            "SLO_UNHEALTHY_WINDOW_SECONDS": float,
            "SLO_ENABLE_RESOURCE_PREDICTION": parse_bool_envar,
            "SLO_CPU_LATENCY_CORRELATION": float,
            "SLO_MEMORY_LATENCY_CORRELATION": float,
            "SLO_PREDICTION_BLEND_WEIGHT": float,
            "SLO_GOSSIP_SUMMARY_TTL_SECONDS": float,
            "SLO_GOSSIP_MAX_JOBS_PER_HEARTBEAT": int,
            # Capacity spillover settings (AD-43)
            "SPILLOVER_MAX_WAIT_SECONDS": float,
            "SPILLOVER_MAX_LATENCY_PENALTY_MS": float,
            "SPILLOVER_MIN_IMPROVEMENT_RATIO": float,
            "SPILLOVER_ENABLED": parse_bool_envar,
            "CAPACITY_STALENESS_THRESHOLD_SECONDS": float,
            # Routing load factor weights (AD-36)
            "ROUTING_UTILIZATION_WEIGHT": float,
            "ROUTING_QUEUE_WEIGHT": float,
            "ROUTING_CIRCUIT_PRESSURE_WEIGHT": float,
            # Discovery settings (AD-28)
            "DISCOVERY_DNS_NAMES": str,
            "DISCOVERY_DNS_CACHE_TTL": float,
            "DISCOVERY_DNS_TIMEOUT": float,
            "DISCOVERY_DEFAULT_PORT": int,
            "DISCOVERY_DNS_ALLOWED_CIDRS": str,
            "DISCOVERY_DNS_BLOCK_PRIVATE_FOR_PUBLIC": parse_bool_envar,
            "DISCOVERY_DNS_DETECT_IP_CHANGES": parse_bool_envar,
            "DISCOVERY_DNS_MAX_IP_CHANGES": int,
            "DISCOVERY_DNS_IP_CHANGE_WINDOW": float,
            "DISCOVERY_DNS_REJECT_ON_VIOLATION": parse_bool_envar,
            "DISCOVERY_DATACENTER_ID": str,
            "DISCOVERY_REGION_ID": str,
            "DISCOVERY_PREFER_SAME_DC": parse_bool_envar,
            "DISCOVERY_CANDIDATE_SET_SIZE": int,
            "DISCOVERY_EWMA_ALPHA": float,
            "DISCOVERY_BASELINE_LATENCY_MS": float,
            "DISCOVERY_LATENCY_MULTIPLIER_THRESHOLD": float,
            "DISCOVERY_MIN_PEERS_PER_TIER": int,
            "DISCOVERY_MAX_CONCURRENT_DNS_RESOLUTIONS": int,
            "DISCOVERY_PROBE_INTERVAL": float,
            "DISCOVERY_FAILURE_DECAY_INTERVAL": float,
        }

    def get_swim_init_context(self) -> dict:
        """
        Get SWIM protocol init_context from environment settings.

        Note (AD-46): Node state is stored in IncarnationTracker.node_states,
        NOT in a 'nodes' queue dict. The legacy queue pattern has been removed.
        """
        return {
            "max_probe_timeout": self.SWIM_MAX_PROBE_TIMEOUT,
            "min_probe_timeout": self.SWIM_MIN_PROBE_TIMEOUT,
            "current_timeout": self.SWIM_CURRENT_TIMEOUT,
            "udp_poll_interval": self.SWIM_UDP_POLL_INTERVAL,
            "suspicion_min_timeout": self.SWIM_SUSPICION_MIN_TIMEOUT,
            "suspicion_max_timeout": self.SWIM_SUSPICION_MAX_TIMEOUT,
            "no_witness_suspicion_timeout": self.SWIM_NO_WITNESS_SUSPICION_TIMEOUT,
            "refutation_rate_limit_tokens": self.SWIM_REFUTATION_RATE_LIMIT_TOKENS,
            "refutation_rate_limit_window": self.SWIM_REFUTATION_RATE_LIMIT_WINDOW,
        }

    def get_circuit_breaker_config(self) -> dict:
        """Get circuit breaker configuration from environment settings."""
        return {
            "max_errors": self.CIRCUIT_BREAKER_MAX_ERRORS,
            "window_seconds": self.CIRCUIT_BREAKER_WINDOW_SECONDS,
            "half_open_after": self.CIRCUIT_BREAKER_HALF_OPEN_AFTER,
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
            "heartbeat_interval": self.LEADER_HEARTBEAT_INTERVAL,
            "election_timeout_base": self.LEADER_ELECTION_TIMEOUT_BASE,
            "election_timeout_jitter": self.LEADER_ELECTION_TIMEOUT_JITTER,
            "pre_vote_timeout": self.LEADER_PRE_VOTE_TIMEOUT,
            "lease_duration": self.LEADER_LEASE_DURATION,
            "max_leader_lhm": self.LEADER_MAX_LHM,
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
            "probe_interval": self.FEDERATED_PROBE_INTERVAL,
            "probe_timeout": self.FEDERATED_PROBE_TIMEOUT,
            "suspicion_timeout": self.FEDERATED_SUSPICION_TIMEOUT,
            "max_consecutive_failures": self.FEDERATED_MAX_CONSECUTIVE_FAILURES,
        }

    def get_overload_config(self):
        """
        Get overload detection configuration (AD-18).

        Creates an OverloadConfig instance from environment settings.
        Uses hybrid detection combining delta-based, absolute bounds,
        and resource-based (CPU/memory) signals.
        """
        from hyperscale.distributed.reliability.overload import OverloadConfig

        return OverloadConfig(
            ema_alpha=self.OVERLOAD_EMA_ALPHA,
            current_window=self.OVERLOAD_CURRENT_WINDOW,
            trend_window=self.OVERLOAD_TREND_WINDOW,
            min_samples=self.OVERLOAD_MIN_SAMPLES,
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

    def get_worker_health_manager_config(self):
        """
        Get worker health manager configuration (AD-26).

        Controls deadline extension tracking for workers.
        Extensions use logarithmic decay to prevent indefinite extensions.
        """
        from hyperscale.distributed.health.worker_health_manager import (
            WorkerHealthManagerConfig,
        )

        return WorkerHealthManagerConfig(
            base_deadline=self.EXTENSION_BASE_DEADLINE,
            min_grant=self.EXTENSION_MIN_GRANT,
            max_extensions=self.EXTENSION_MAX_EXTENSIONS,
            eviction_threshold=self.EXTENSION_EVICTION_THRESHOLD,
            warning_threshold=self.EXTENSION_EXHAUSTION_WARNING_THRESHOLD,
            grace_period=self.EXTENSION_EXHAUSTION_GRACE_PERIOD,
        )

    def get_extension_tracker_config(self):
        """
        Get extension tracker configuration (AD-26).

        Creates configuration for per-worker extension trackers.
        """
        from hyperscale.distributed.health.extension_tracker import (
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
        from hyperscale.distributed.datacenters.cross_dc_correlation import (
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
        node_role: str = "worker",
        static_seeds: list[str] | None = None,
        allow_dynamic_registration: bool = False,
    ):
        """
        Get discovery service configuration (AD-28).

        Creates configuration for peer discovery, locality-aware selection,
        and adaptive load balancing, filtering peers by this node's
        CLUSTER_ID and ENVIRONMENT_ID.

        Args:
            node_role: Role of the local node ('worker', 'manager', etc.)
            static_seeds: Static seed addresses in "host:port" format
            allow_dynamic_registration: Allow empty seeds (peers register dynamically)
        """
        from hyperscale.distributed.discovery.models.discovery_config import (
            DiscoveryConfig,
        )

        # Parse DNS names from comma-separated string (StrictStr: "" splits
        # to [""], which the blank filter drops, so no emptiness guard).
        dns_names: list[str] = list(filter(None, map(str.strip, self.DISCOVERY_DNS_NAMES.split(","))))

        # Parse allowed CIDRs from comma-separated string
        dns_allowed_cidrs: list[str] = list(
            filter(None, map(str.strip, self.DISCOVERY_DNS_ALLOWED_CIDRS.split(",")))
        )

        return DiscoveryConfig(
            cluster_id=self.CLUSTER_ID,
            environment_id=self.ENVIRONMENT_ID,
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
            max_concurrent_dns_resolutions=self.DISCOVERY_MAX_CONCURRENT_DNS_RESOLUTIONS,
            # Dynamic registration mode
            allow_dynamic_registration=allow_dynamic_registration,
        )

    def get_pending_response_config(self) -> dict:
        """
        Get bounded pending response configuration (AD-32).

        Returns configuration for the priority-aware bounded execution system:
        - Per-priority limits (CRITICAL unlimited unless a hook sets a
          bounded admission group; HIGH/NORMAL/LOW bounded)
        - Global limit across all priorities
        - Load shedding: LOW shed first, then NORMAL, then HIGH
        - SWIM uses a dedicated bounded reserve instead of NORMAL capacity

        This prevents memory exhaustion under high load while:
        - Keeping SWIM protocol work isolated from DATA/NORMAL load shedding
        - Providing graceful degradation (shed stats before job commands)
        - Enabling immediate execution (no queue latency for most messages)
        """
        return {
            "global_limit": self.PENDING_RESPONSE_MAX_CONCURRENT,
            "swim_limit": self.PENDING_RESPONSE_SWIM_LIMIT,
            "high_limit": self.PENDING_RESPONSE_HIGH_LIMIT,
            "normal_limit": self.PENDING_RESPONSE_NORMAL_LIMIT,
            "low_limit": self.PENDING_RESPONSE_LOW_LIMIT,
            "warn_threshold": self.PENDING_RESPONSE_WARN_THRESHOLD,
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
            "queue_size": self.OUTGOING_QUEUE_SIZE,
            "overflow_size": self.OUTGOING_OVERFLOW_SIZE,
            "max_destinations": self.OUTGOING_MAX_DESTINATIONS,
        }
