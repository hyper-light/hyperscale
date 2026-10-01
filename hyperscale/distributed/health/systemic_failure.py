from __future__ import annotations


def is_systemic_failure(failing_count: int, population: int) -> bool:
    """AD-19: whether ``failing_count`` of ``population`` nodes failing at
    once looks systemic -- the observer's own network, clock or load, not
    the nodes -- so evicting them would be a cascade, not a repair.

    More than half the population failing together is systemic. One
    failure alone is never evidence of correlation, so a single failing
    node (even the only one) is always judged on its own.
    """
    return failing_count >= 2 and failing_count * 2 > population
