"""Wire model ``HealthcheckExtensionResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class HealthcheckExtensionResponse(Message):
    """
    Response to a healthcheck extension request (AD-26).

    If granted, the worker's deadline is extended by extension_seconds.
    If denied, the denial_reason explains why.

    Extensions may be denied if:
    - Maximum extensions already granted
    - No progress since last extension
    - Worker is being evicted

    Graceful exhaustion:
    - is_exhaustion_warning: True when close to exhaustion (remaining <= threshold)
    - grace_period_remaining: Seconds of grace time left after exhaustion
    - in_grace_period: True if exhausted but still within grace period

    Sent from: Manager -> Worker
    """

    granted: bool  # Whether extension was granted
    extension_seconds: float  # Seconds of extension granted (0 if denied)
    new_deadline: float  # New deadline timestamp (if granted)
    remaining_extensions: int  # Number of extensions remaining
    denial_reason: str | None = None  # Why extension was denied
    is_exhaustion_warning: bool = False  # True if about to exhaust extensions
    grace_period_remaining: float = 0.0  # Seconds of grace remaining after exhaustion
    in_grace_period: bool = False  # True if exhausted but within grace period
    # Phase H5 — structured denial code (string-valued so the wire
    # format stays stable across enum extensions). One of:
    # "none" / "max_exhausted" / "counter_regression" / "no_advancement"
    # / "throughput_regime_down" / "overloaded_state" / "rate_limited".
    # Workers can branch on this without parsing the free-text
    # ``denial_reason``; observability tooling (Phase H7 ledger,
    # Phase H8 outcome feedback) consumes it as the canonical
    # category.
    denial_reason_code: str = "none"
