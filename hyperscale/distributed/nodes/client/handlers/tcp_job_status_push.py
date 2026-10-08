"""
TCP handler for job status push notifications.

Handles JobStatusPush and JobBatchPush messages from gates/managers.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import inspect
from hyperscale.distributed.models import JobStatusPush, JobBatchPush
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.status_application import JobStatusApplier
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerWarning

from .job_batch_push_handler import JobBatchPushHandler
from .job_status_push_handler import JobStatusPushHandler

_REHOMED = (
    JobStatusPushHandler,
    JobBatchPushHandler,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
