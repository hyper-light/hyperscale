"""``ExtensionOutcomeKind`` -- pickled under the namespace
``hyperscale.distributed.health.extension_outcome`` (see that module)."""

from __future__ import annotations

from enum import Enum


class ExtensionOutcomeKind(Enum):
    """How a workflow with extension history terminated.

    The Bayesian tuner treats COMPLETED as a positive Bernoulli
    observation (the extension(s) helped) and TIMED_OUT / FAILED /
    EVICTED as negative observations (the extension(s) didn't
    rescue the workflow). UNKNOWN is reserved for outcomes we
    haven't classified yet — it does not contribute to the
    posterior.
    """

    UNKNOWN = "unknown"
    COMPLETED = "completed"
    TIMED_OUT = "timed_out"
    FAILED = "failed"
    EVICTED = "evicted"

    @property
    def is_success(self) -> bool:
        """True iff this outcome counts as evidence the extensions
        were warranted."""
        return self is ExtensionOutcomeKind.COMPLETED

    @property
    def contributes_to_posterior(self) -> bool:
        """True iff this outcome is informative for the Bayesian
        tuner (i.e. excludes UNKNOWN)."""
        return self is not ExtensionOutcomeKind.UNKNOWN
