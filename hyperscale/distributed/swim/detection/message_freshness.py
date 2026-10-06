"""``MessageFreshness`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.incarnation_tracker`` (see that module)."""

from enum import Enum


class MessageFreshness(Enum):
    """
    Result of checking message freshness.

    Indicates whether a message should be processed and why it was
    accepted or rejected. This enables appropriate handling per case.
    """

    FRESH = "fresh"
    """Message has new information - process it."""

    DUPLICATE = "duplicate"
    """Same incarnation and same/lower status priority - silent ignore.
    This is completely normal in gossip protocols where the same state
    propagates via multiple paths."""

    STALE = "stale"
    """Lower incarnation than known - indicates delayed message or state drift.
    Worth logging as it may indicate network issues."""

    INVALID = "invalid"
    """Incarnation number failed validation (negative or exceeds max).
    Indicates bug or corruption."""

    SUSPICIOUS = "suspicious"
    """Incarnation jump is suspiciously large - possible attack or serious bug."""
