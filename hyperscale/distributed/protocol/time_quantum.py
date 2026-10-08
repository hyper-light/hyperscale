"""
Time-remainder epsilon — the protocol-level contract for "how close to
a deadline counts as the deadline".

Deadlines all over the dispatch tier are COMPOSED floats
(``anchor + delay`` against a monotonic ``now``), so a remainder
``deadline - now`` can be a positive sub-quantum artifact of float
arithmetic (observed live: 1.6e-11 seconds at a frozen virtual instant
of ~22s magnitude) even though the deadline has semantically arrived.
Any code that treats such a remainder as "not yet due" while another
path WAITS on it re-arms a timer that cannot advance a quantized
clock: eligibility and waiting disagree forever — the dispatcher
frozen-instant livelock (100%-CPU micro-spin on a real host, hard
livelock under deterministic simulation).

The contract: a remainder at or below ``TIME_REMAINDER_EPSILON_SECONDS``
IS expiry. Every expiry predicate and every remaining-time computation
derived from the same deadline must apply it, so nothing ever waits on
a remainder too small to schedule.

One microsecond sits three orders of magnitude above the float-artifact
scale at realistic monotonic magnitudes (float64 ulp at 1e6 seconds is
~2e-10) and three below the smallest meaningful scheduling delay in
this codebase (the 1ms wait floors), so it can neither misclassify a
real wait nor let an artifact through.
"""

TIME_REMAINDER_EPSILON_SECONDS: float = 1e-6
