"""``DNSError`` -- pickled under the namespace
``hyperscale.distributed.discovery.dns.resolver`` (see that module)."""



class DNSError(Exception):
    """Raised when DNS resolution fails."""

    def __init__(self, hostname: str, message: str):
        self.hostname = hostname
        super().__init__(f"DNS resolution failed for '{hostname}': {message}")
