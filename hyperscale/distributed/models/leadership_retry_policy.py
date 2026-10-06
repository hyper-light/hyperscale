"""Wire model ``LeadershipRetryPolicy`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class LeadershipRetryPolicy:
    """
    Configurable retry behavior for leadership changes (Section 9.3.3).

    Controls how clients retry operations when leadership changes occur.
    """

    max_retries: int = 3
    retry_delay: float = 0.5
    exponential_backoff: bool = True
    max_delay: float = 5.0
