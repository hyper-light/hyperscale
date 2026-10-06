"""Definitions shared by the classes of
``hyperscale.distributed.swim.retry`` (see that module)."""

from hyperscale.distributed.runtime import Random, RealRandom

_DEFAULT_RANDOM: Random = RealRandom()
