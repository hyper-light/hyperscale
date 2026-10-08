class ClusterCookieUnavailableError(Exception):
    """The per-user cluster cookie could not be created, read or trusted.

    The message names the reason and the two ways an operator can give the
    cluster its secret instead, so a command never falls back to running
    without one.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(
            f"hyperscale could not use the per-user cluster cookie: {reason}. "
            "Set the MERCURY_SYNC_AUTH_SECRET environment variable or pass --acm-secret "
            "with the secret every node of the cluster shares."
        )
