"""``MessageSizeError`` -- pickled under the namespace
``hyperscale.distributed.server.protocol.security`` (see that module)."""



class MessageSizeError(Exception):
    """Raised when message size limits are exceeded."""
    pass
