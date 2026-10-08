from __future__ import annotations


class ClockOffsetProbeError(Exception):
    """A clock offset probe got no usable reply: the peer was unreachable,
    refused, or answered with something that is not a probe reply."""
