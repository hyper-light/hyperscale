from enum import Enum


class ReadConsistency(str, Enum):
    """How current a job status read must be (AD-38 Part 8).

    * EVENTUAL -- whatever the asked node holds; may be stale.
    * SESSION -- at least as new as anything this reader already saw of
      the job (read-your-writes, monotonic reads).
    * BOUNDED_STALENESS -- the job's leader's view, at most a stated age.
    * STRONG -- the job's leader's view, confirmed current: the leader
      re-asserts its leadership to a quorum before answering.

    A node that cannot answer at the level asked forwards the read to the
    job's leader. A terminal status never changes, so any copy of one
    answers every level.
    """

    EVENTUAL = "eventual"
    SESSION = "session"
    BOUNDED_STALENESS = "bounded_staleness"
    STRONG = "strong"
