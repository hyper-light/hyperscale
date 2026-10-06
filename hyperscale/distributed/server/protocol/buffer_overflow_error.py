"""``BufferOverflowError`` -- pickled under the namespace
``hyperscale.distributed.server.protocol.receive_buffer`` (see that module)."""

from __future__ import annotations



class BufferOverflowError(Exception):
    """Raised when buffer size limits are exceeded."""
    pass
