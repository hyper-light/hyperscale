"""
AD-24 per-client rate limits, derived from the protocol rates they bound.

Every limit counts one peer's requests of one operation in a sliding window
of W seconds (``derive_rate_limit_window_seconds``). A sender that sends at
most once per interval T sends at most floor(W / T) + 1 requests in any span
W long: one may sit at each end of the span.

The counter (``SlidingWindowCounter``) estimates a span's count as the
current window's count plus the previous window's count weighted by the part
of the previous window still inside the span. A sender within N requests per
span holds at most N in the current window (shorter than a span) and at most
N in the previous one, so its estimate never passes 2N -- and reaches 2N when
the previous window's N requests all sat at its start. Each limit is
therefore ``SLIDING_WINDOW_ESTIMATE_BOUND`` times the protocol's maximum: a
legitimate sender at its protocol's maximum rate is never refused, and one
sustaining more than twice that rate always is.

An operation whose rate the protocol bounds neither by an interval nor by a
configured concurrency -- a request-driven one (submissions, status reads,
cancellations, per-job reports and pushes, registrations) whose volume grows
with the jobs a caller runs, which no setting caps -- has no legitimate
maximum to derive. Its limit is ``UNBOUNDED_REQUESTS`` unless an operator
configures one, and a flood of it is shed by the health-gated states instead:
the STRESSED per-client budget (``derive_stressed_max_requests``) and
OVERLOADED priority shedding.

Each limit is an Env setting; unset (None), it takes the derivation below.
"""

import math
import os
import sys

from hyperscale.distributed.env.env import Env

# The counter's estimate of a legitimate sender's span count is at most
# twice the sender's true maximum (module docstring).
SLIDING_WINDOW_ESTIMATE_BOUND = 2

# No per-window limit: the counter's estimate never reaches it.
UNBOUNDED_REQUESTS = sys.maxsize

MILLISECONDS_PER_SECOND = 1000.0


def derive_rate_limit_window_seconds(env: Env) -> float:
    """The window every per-client count spans: ``RATE_LIMIT_WINDOW_SECONDS``,
    else the span the node's overload detector averages its load over
    (``OVERLOAD_CURRENT_WINDOW`` samples, one per
    ``OVERLOAD_SAMPLE_INTERVAL_SECONDS``) -- a client's rate is judged over
    the same horizon as the node's own load (10s by default)."""
    configured = env.RATE_LIMIT_WINDOW_SECONDS
    return configured if configured is not None else env.OVERLOAD_CURRENT_WINDOW * env.OVERLOAD_SAMPLE_INTERVAL_SECONDS


def derive_worker_cores(env: Env) -> int:
    """The most cores a worker runs workflows on, so the most workflows it
    runs at once (each holds at least one core): ``WORKER_MAX_CORES``, else
    this host's logical core count -- an upper bound of the physical count a
    worker on this host allocates. A fleet whose workers have more cores than
    the host enforcing the limit sets ``WORKER_MAX_CORES`` (or the limit)."""
    return env.WORKER_MAX_CORES or os.cpu_count() or 1


def derive_heartbeat_max_requests(env: Env) -> int:
    """``heartbeat`` (``manager_status_update`` manager->gate,
    ``manager_resource_gossip`` manager->manager): one per peer per
    ``MANAGER_HEARTBEAT_INTERVAL``, from a loop that sleeps the interval
    between sends and never retries -- floor(W / interval) + 1 per span,
    times the estimate bound. 6 by default (W=10s, interval 5s)."""
    configured = env.RATE_LIMIT_HEARTBEAT_MAX_REQUESTS
    window_seconds = derive_rate_limit_window_seconds(env)
    sends_per_span = math.floor(window_seconds / env.MANAGER_HEARTBEAT_INTERVAL) + 1
    return configured if configured is not None else SLIDING_WINDOW_ESTIMATE_BOUND * sends_per_span


def derive_progress_update_max_requests(env: Env) -> int:
    """``progress_update`` (``workflow_progress`` worker->manager): a worker's
    flush loop sleeps ``WORKER_PROGRESS_FLUSH_INTERVAL`` between passes and
    sends each running workflow's latest update once per pass, without
    retries, to that workflow's leader (or, the leader failing, to one other
    manager) -- at most one request per running workflow per pass at any
    manager. A worker runs at most ``derive_worker_cores`` workflows:
    cores * (floor(W / flush) + 1) per span, times the estimate bound
    (402 per core by default)."""
    configured = env.RATE_LIMIT_PROGRESS_UPDATE_MAX_REQUESTS
    window_seconds = derive_rate_limit_window_seconds(env)
    passes_per_span = math.floor(window_seconds / env.WORKER_PROGRESS_FLUSH_INTERVAL) + 1
    derived = SLIDING_WINDOW_ESTIMATE_BOUND * derive_worker_cores(env) * passes_per_span
    return configured if configured is not None else derived


def derive_stressed_max_requests(env: Env) -> int:
    """Every client's budget across all its operations while the node is
    STRESSED: what its most active legitimate peer sends once AD-37
    backpressure throttles it -- a worker's progress for each of its
    workflows once per flush plus ``WORKER_BACKPRESSURE_THROTTLE_DELAY_MS``,
    and a manager's heartbeat once per ``MANAGER_HEARTBEAT_INTERVAL`` -- per
    span, times the estimate bound. Everything a client sends past it while
    the node is stressed (a submission storm, a read flood) is refused."""
    configured = env.RATE_LIMIT_STRESSED_MAX_REQUESTS
    window_seconds = derive_rate_limit_window_seconds(env)
    throttled_flush_seconds = (
        env.WORKER_PROGRESS_FLUSH_INTERVAL + env.WORKER_BACKPRESSURE_THROTTLE_DELAY_MS / MILLISECONDS_PER_SECOND
    )
    throttled_progress = derive_worker_cores(env) * (math.floor(window_seconds / throttled_flush_seconds) + 1)
    heartbeats = math.floor(window_seconds / env.MANAGER_HEARTBEAT_INTERVAL) + 1
    derived = SLIDING_WINDOW_ESTIMATE_BOUND * (throttled_progress + heartbeats)
    return configured if configured is not None else derived


def derive_max_tracked_clients(env: Env, accepted_connection_cap: int | None) -> int:
    """The most clients the limiter tracks before evicting the least recently
    active: ``RATE_LIMIT_MAX_TRACKED_CLIENTS``, else two per connection the
    node's TCP server holds at once (``accepted_connection_cap``) -- each
    connection is counted under its peer address by the transport, and under
    the address its requests name by the handlers that limit by sender.
    Unbounded where the server holds connections without a cap (no
    descriptor limit): idle clients still leave after
    ``RATE_LIMIT_CLIENT_IDLE_TIMEOUT``."""
    configured = env.RATE_LIMIT_MAX_TRACKED_CLIENTS
    derived = 2 * accepted_connection_cap if accepted_connection_cap is not None else UNBOUNDED_REQUESTS
    return configured if configured is not None else derived


def configured_or_unbounded(configured: int | None) -> int:
    """A request-driven operation's limit: as configured, else unbounded --
    the protocol caps neither its rate nor its concurrency (module
    docstring)."""
    return configured if configured is not None else UNBOUNDED_REQUESTS


def derive_operation_limits(env: Env) -> dict[str, tuple[int, float]]:
    """Every AD-24 operation's (max requests, window seconds) per client."""
    window_seconds = derive_rate_limit_window_seconds(env)
    return {
        "heartbeat": (derive_heartbeat_max_requests(env), window_seconds),
        "progress_update": (derive_progress_update_max_requests(env), window_seconds),
        "stats_update": (configured_or_unbounded(env.RATE_LIMIT_STATS_UPDATE_MAX_REQUESTS), window_seconds),
        "job_submit": (configured_or_unbounded(env.RATE_LIMIT_JOB_SUBMIT_MAX_REQUESTS), window_seconds),
        "job_status": (configured_or_unbounded(env.RATE_LIMIT_JOB_STATUS_MAX_REQUESTS), window_seconds),
        "workflow_dispatch": (
            configured_or_unbounded(env.RATE_LIMIT_WORKFLOW_DISPATCH_MAX_REQUESTS),
            window_seconds,
        ),
        "cancel": (configured_or_unbounded(env.RATE_LIMIT_CANCEL_MAX_REQUESTS), window_seconds),
        "reconnect": (configured_or_unbounded(env.RATE_LIMIT_RECONNECT_MAX_REQUESTS), window_seconds),
        "default": (configured_or_unbounded(env.RATE_LIMIT_DEFAULT_MAX_REQUESTS), window_seconds),
    }
