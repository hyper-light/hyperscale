"""Extension outcome events (AD-26 Phase H8a).

When a workflow terminates, the leader records ``how it ended`` —
completed normally, timed out, failed, or was evicted — and pairs
the outcome with the cluster-wide record of every extension
decision that workflow accumulated. The outcome event is the
training signal for the H8 Bayesian alpha-tuner: workflows that
extended-and-completed shrink the false-positive budget for that
workflow class; workflows that extended-and-timed-out widen it so
future requests of the same class face stricter scrutiny.

Wire-level events are disseminated via AD-48 piggyback on a new
``#|o`` channel parallel to H7b's ``#|x`` decision channel. The
on-the-wire format is ``:``-delimited primitives so the event can
be safely pickled, hashed, and replayed on a peer manager.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum

from .extension_outcome_event import _OUTCOME_FIELD_COUNT
from .extension_outcome_event import ExtensionOutcomeEvent
from .extension_outcome_kind import ExtensionOutcomeKind

_REHOMED = (
    ExtensionOutcomeKind,
    ExtensionOutcomeEvent,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
