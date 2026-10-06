"""

This module is the wire namespace of the models below. Each lives in a
file of its own and is re-homed here -- its ``__module__`` set to this
module -- so its pickled form names this module, exactly as before the
split: mixed-version clusters keep talking and data written earlier
keeps loading.
"""

from .ack import Ack
from .confirm import Confirm
from .eject import Eject
from .join import Join
from .leave import Leave
from .nack import Nack
from .probe import Probe

_WIRE_MODELS = (
    Ack,
    Confirm,
    Eject,
    Join,
    Leave,
    Nack,
    Probe,
)

for _wire_model in _WIRE_MODELS:
    _wire_model.__module__ = __name__
