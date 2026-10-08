from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class ClockFenceVerdict:
    """One evaluation of whether this node's clock may be trusted to
    mint timestamps.

    ``peers_beyond`` peers measured this node's offset certainly beyond
    ``threshold_ms``; ``quorum`` of them fence it. ``hlc_lead_ms`` is how
    far the node's own HLC runs ahead of its physical clock: merging a
    peer's in-bound timestamp leads it by at most the offset bound, so a
    lead beyond the bound means this node's own clock stepped back (or
    ran fast before) -- its timestamps would be refused, and it fences.
    """

    fenced: bool
    peers_beyond: int
    peers_measured: int
    quorum: int
    hlc_lead_ms: int
    threshold_ms: int
