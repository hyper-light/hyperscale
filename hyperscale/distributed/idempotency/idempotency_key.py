"""

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import count
import secrets

from .idempotency_key_model import IdempotencyKey
from .idempotency_key_generator import IdempotencyKeyGenerator

_REHOMED = (
    IdempotencyKey,
    IdempotencyKeyGenerator,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
