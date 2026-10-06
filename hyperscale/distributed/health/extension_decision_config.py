"""``ExtensionDecisionConfig`` -- pickled under the namespace
``hyperscale.distributed.health.extension_decision`` (see that module)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from .extension_decision_evaluator import ExtensionDecisionEvaluator


@dataclass(slots=True, frozen=True)
class ExtensionDecisionConfig:
    """Configuration for ``ExtensionDecisionEvaluator``."""

    # Rate-limit between successive extension grants on the same
    # worker. Defaults to half the AD-26 base_deadline (15s) so that
    # the cumulative time-to-exhaust spans at least a heartbeat-
    # interval-multiple even when every extension is granted as
    # soon as the rate limit allows. Prevents extension storms
    # within a single heartbeat-processing window.
    min_between_extensions_seconds: float = 15.0
