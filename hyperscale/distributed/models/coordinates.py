"""

This module is the wire namespace of the models below. Each lives in a
file of its own and is re-homed here -- its ``__module__`` set to this
module -- so its pickled form names this module, exactly as before the
split: mixed-version clusters keep talking and data written earlier
keeps loading.
"""

from .network_coordinate import NetworkCoordinate
from .vivaldi_config import VivaldiConfig

_WIRE_MODELS = (
    VivaldiConfig,
    NetworkCoordinate,
)

for _wire_model in _WIRE_MODELS:
    _wire_model.__module__ = __name__
