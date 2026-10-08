"""
TCP handlers for leadership transfer notifications.

Handles GateJobLeaderTransfer and ManagerJobLeaderTransfer messages.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from hyperscale.distributed.models import (
    GateJobLeaderTransfer,
    GateJobLeaderTransferAck,
    ManagerJobLeaderTransfer,
    ManagerJobLeaderTransferAck,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError

from .tcp_leadership_transfer_shared import _addr_str
from .gate_leader_transfer_handler import GateLeaderTransferHandler
from .manager_leader_transfer_handler import ManagerLeaderTransferHandler

_REHOMED = (
    GateLeaderTransferHandler,
    ManagerLeaderTransferHandler,
    _addr_str,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
