"""Wire model ``SecurityError`` -- pickled under the wire namespace
``hyperscale.distributed.models.restricted_unpickler`` (see that module)."""



class SecurityError(Exception):
    """Raised when deserialization attempts to load blocked modules/classes."""
    pass
