"""``PowerOfTwoConfig`` -- pickled under the namespace
``hyperscale.distributed.discovery.selection.adaptive_selector`` (see that module)."""

from dataclasses import dataclass


@dataclass
class PowerOfTwoConfig:
    """Configuration for Power of Two Choices selection."""

    candidate_count: int = 2
    """
    Number of candidates to consider (k in "power of k choices").

    More candidates = better load balancing, but less cache locality.
    - 2: Classic "power of two" (good balance)
    - 3-4: Better load balancing for hot keys
    - 1: Degrades to pure rendezvous hash (no load awareness)
    """

    use_rendezvous_ranking: bool = True
    """
    If True, candidates are top-k from rendezvous hash.
    If False, candidates are randomly selected.

    Rendezvous ranking provides better cache locality and
    deterministic fallback ordering.
    """

    latency_threshold_ms: float = 100.0
    """
    If best EWMA latency is below this, skip load-aware selection.

    Avoids unnecessary overhead when all peers are healthy.
    """

    random_seed: int | None = None
    """Optional seed for random selection (for testing)."""
