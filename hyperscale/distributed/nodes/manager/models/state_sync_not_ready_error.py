class StateSyncNotReadyError(Exception):
    """A state sync target answered that it has not finished its own
    startup (``responder_ready=False``): the requester retries after a
    delay, as ``StateSyncResponse`` specifies."""
