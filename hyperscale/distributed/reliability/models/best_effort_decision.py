"""
A best-effort job's completion decision (AD-44).
"""

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class BestEffortDecision:
    """Whether a best-effort job completes now (``should_complete``), why
    (``reason``), whether as a success (``success``), and whether the result
    is ``provisional``: under the ``update`` late-result policy a job that
    reached its ``min_dcs`` hands out its result while its unreported
    datacenters run on, and completes for good when they all reported or
    its deadline passed."""

    should_complete: bool
    reason: str
    success: bool
    provisional: bool = False
