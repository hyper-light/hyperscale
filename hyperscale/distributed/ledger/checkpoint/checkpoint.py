"""

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

import asyncio
import struct
import zlib
from pathlib import Path
from typing import Any, TYPE_CHECKING
import msgspec
from hyperscale.distributed.runtime import Filesystem, RealFilesystem
from hyperscale.logging.hyperscale_logging_models import CheckpointRetentionError
from hyperscale.distributed.ledger.storage_format import (
    StorageFormat,
    UnrecognizedStorageFormatError,
    set_aside_unrecognized,
)
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .checkpoint_manager import CHECKPOINT_FORMAT
from .checkpoint_manager import CHECKPOINT_HEADER_SIZE
from .checkpoint_manager import _DEFAULT_FILESYSTEM
from .checkpoint_model import Checkpoint
from .checkpoint_manager import CheckpointManager

_REHOMED = (
    Checkpoint,
    CheckpointManager,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
