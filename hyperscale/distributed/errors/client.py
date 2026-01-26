"""
Client-specific exceptions for the Hyperscale distributed system.

These exceptions are raised by the HyperscaleClient during job submission
and other client operations.
"""


class MessageTooLargeError(Exception):
    """
    Raised when a message exceeds the maximum allowed size before submission.

    This is a client-side pre-submission validation error that prevents
    sending messages that would be rejected by the server. Failing fast
    on the client side provides a better user experience than waiting
    for a server rejection.

    The default limit is MAX_DECOMPRESSED_SIZE (5MB) from
    hyperscale.core.jobs.protocols.constants.
    """
    pass
