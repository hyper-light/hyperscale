"""
Client configuration: ``ClientConfig`` lives in
``nodes/client/models/client_config.py``.
"""

# Transient errors that should trigger retry logic (AD-21, AD-32)
# Includes cluster state errors and load shedding/rate limiting patterns
# The transient-rejection vocabulary is a protocol-level contract shared
# by every hop that classifies JobAck rejections (client submitters AND
# the gate's datacenter dispatch); it lives in
# ``hyperscale.distributed.protocol.transient_errors`` and is re-exported
# here for the existing client-side importers.
from hyperscale.distributed.protocol.transient_errors import (
    TRANSIENT_ERRORS as TRANSIENT_ERRORS,
)
