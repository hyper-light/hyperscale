"""
CompletionNoticeObligation — one owed ``JobFinalResult`` delivery from a
manager to a job's origin gate.

A job's completion notice used to be a single 5s-timeout send followed
unconditionally by job-state cleanup: a gate<->manager partition (or a
dead/restarting gate) covering the completion instant lost the
completion FOREVER, and the gate's AD-34 tracker resolved the job as
``timeout`` even though the workflow ran to success (measured live in
the multi-DC fault program). Under this contract the notice is an
OBLIGATION instead: registered when the first send fails, carried with
the fully SERIALIZED payload (job state is cleaned up immediately after
— the obligation must not depend on any state the cleanup erases), and
re-sent on capped exponential backoff until the gate acks, the gate
explicitly rejects, or the age ceiling expires (a gone-forever gate;
its own AD-34 tracker terminal-resolved the job long before, so a
delivery after that point is a duplicate, not a correction — the drop
is logged loudly either way).
"""

from dataclasses import dataclass


@dataclass(slots=True)
class CompletionNoticeObligation:
    """One owed completion notice, self-contained for resend."""

    job_id: str
    origin_gate_addr: tuple[str, int]
    payload: bytes
    """``JobFinalResult.dump()`` captured at completion time — resends
    must not rebuild it (cleanup already erased the source state, and
    the terminal payload is immutable by definition)."""

    created_at: float
    """Monotonic instant the obligation was registered."""

    last_attempt_at: float
    """Monotonic instant of the most recent send attempt."""

    attempt_count: int = 1
    """Send attempts so far (the failed initial send counts as 1)."""

    def next_attempt_due(
        self,
        base_interval_seconds: float,
        max_interval_seconds: float,
    ) -> float:
        """Monotonic instant the next resend is due — capped
        exponential backoff off the attempt count."""
        backoff_seconds = min(
            base_interval_seconds * (2 ** max(0, self.attempt_count - 1)),
            max_interval_seconds,
        )
        return self.last_attempt_at + backoff_seconds

    def expired(self, now: float, max_age_seconds: float) -> bool:
        """Whether the obligation has outlived the age ceiling."""
        return (now - self.created_at) >= max_age_seconds
